import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Router } from '@angular/router';
import {
  BehaviorSubject,
  Observable,
  catchError,
  firstValueFrom,
  map,
  of,
  shareReplay,
  tap,
  timeout,
  throwError,
} from 'rxjs';
import { AuthResponse, PermissionCode, SessionState, WindowCode } from './auth.models';
import { API_BASE_URL } from './api-base';
import { firstAllowedPath } from './navigation';

const ACCESS_TOKEN_KEY = 'uai.auth.access_token';
const REFRESH_TOKEN_KEY = 'uai.auth.refresh_token';
const SESSION_CACHE_KEY = 'uai.auth.session_cache';

@Injectable({ providedIn: 'root' })
export class AuthService {

  private readonly baseUrl = `${API_BASE_URL}/auth`;

  private readonly authRequestTimeoutMs = 15000;

  private readonly sessionSubject = new BehaviorSubject<SessionState | null>(null);
  private readonly readySubject = new BehaviorSubject(false);
  private refreshRequest$: Observable<string | null> | null = null;

  readonly session$ = this.sessionSubject.asObservable();
  readonly ready$ = this.readySubject.asObservable();

  constructor(
    private readonly http: HttpClient,
    private readonly router: Router,
  ) {}

  async init() {
    const accessToken = localStorage.getItem(ACCESS_TOKEN_KEY);
    const refreshToken = localStorage.getItem(REFRESH_TOKEN_KEY);

    if (!accessToken || !refreshToken) {
      this.clearSession();
      this.readySubject.next(true);
      return;
    }

    try {
      await firstValueFrom(this.fetchMe(accessToken, refreshToken));
    } catch (fetchError: unknown) {
      // Distinguish between auth errors (401/403 → invalid token) and
      // network/server errors (0/5xx → backend temporarily unavailable).
      // On network errors we preserve the stored tokens and restore the
      // cached profile so the user is not logged out due to infra issues.
      const status = (fetchError as { status?: number })?.status ?? 0;
      const isNetworkOrServerError = status === 0 || status >= 500;

      if (isNetworkOrServerError) {
        const cached = this.restoreSessionCache(accessToken, refreshToken);
        if (!cached) {
          // No cache and server unreachable — keep tokens but mark as ready;
          // the interceptor will refresh the token on the next API call.
          this.clearSession(); // removes from memory only if already empty
        }
        // Even if we couldn't restore full profile, don't wipe tokens.
        localStorage.setItem(ACCESS_TOKEN_KEY, accessToken);
        localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken);
      } else {
        // 401/403 or unexpected auth error — try a token refresh.
        try {
          await firstValueFrom(this.refreshAccessToken());
        } catch {
          this.clearSession();
        }
      }
    }

    this.readySubject.next(true);
  }

  login(username: string, password: string) {
    return this.http
      .post<AuthResponse>(`${this.baseUrl}/login`, {
        username,
        password,
      })
      .pipe(
        timeout({ first: this.authRequestTimeoutMs }),
        tap((response) => this.setSessionFromAuthResponse(response)),
        map(() => void 0),
      );
  }

  logout(redirect = true) {
    const refreshToken = this.refreshToken;
    const request$ = this.accessToken
      ? this.http.post(`${this.baseUrl}/logout`, {
          refresh_token: refreshToken,
        })
      : of<unknown>(null);

    return request$.pipe(
      catchError(() => of(null)),
      tap(() => {
        this.clearSession();
        if (redirect) {
          void this.router.navigateByUrl('/login');
        }
      }),
      map(() => void 0),
    );
  }

  fetchMe(accessToken = this.accessToken, refreshToken = this.refreshToken) {
    if (!accessToken || !refreshToken) {
      return throwError(() => new Error('Sesion no disponible.'));
    }
    return this.http
      .get<Omit<AuthResponse, 'access_token' | 'refresh_token'>>(`${this.baseUrl}/me`)
      .pipe(
        timeout({ first: this.authRequestTimeoutMs }),
        tap((response) =>
          this.setSession({
            accessToken,
            refreshToken,
            user: response.user,
            roles: response.roles,
            scopes: response.scopes,
            permissions: response.permissions,
            windows: response.windows,
          }),
        ),
      );
  }

  refreshAccessToken() {
    if (!this.refreshToken) {
      this.clearSession();
      return throwError(() => new Error('No refresh token.'));
    }
    if (!this.refreshRequest$) {
      this.refreshRequest$ = this.http
        .post<AuthResponse>(`${this.baseUrl}/refresh`, {
          refresh_token: this.refreshToken,
        })
        .pipe(
          timeout({ first: this.authRequestTimeoutMs }),
          tap((response) => this.setSessionFromAuthResponse(response)),
          map((response) => response.access_token),
          catchError((error) => {
            this.clearSession();
            return throwError(() => error);
          }),
          shareReplay(1),
        );
      this.refreshRequest$.subscribe({
        next: () => {
          this.refreshRequest$ = null;
        },
        error: () => {
          this.refreshRequest$ = null;
        },
      });
    }
    return this.refreshRequest$;
  }

  hasWindow(window: WindowCode) {
    return this.sessionSubject.value?.windows.includes(window) ?? false;
  }

  hasPermission(permission: PermissionCode) {
    return this.sessionSubject.value?.permissions.includes(permission) ?? false;
  }

  isAuthenticated() {
    return Boolean(this.sessionSubject.value?.accessToken);
  }

  firstAllowedPath() {
    return firstAllowedPath(
      this.sessionSubject.value?.windows ?? [],
      this.sessionSubject.value?.permissions ?? [],
    );
  }

  redirectAfterLogin() {
    const path = this.firstAllowedPath();
    if (!path) {
      this.clearSession();
      return Promise.resolve(false);
    }
    return this.router.navigateByUrl(path);
  }

  get snapshot() {
    return this.sessionSubject.value;
  }

  get accessToken() {
    return this.sessionSubject.value?.accessToken ?? localStorage.getItem(ACCESS_TOKEN_KEY);
  }

  get refreshToken() {
    return this.sessionSubject.value?.refreshToken ?? localStorage.getItem(REFRESH_TOKEN_KEY);
  }

  clearLocalSession() {
    this.clearSession();
  }

  private setSessionFromAuthResponse(response: AuthResponse) {
    this.setSession({
      accessToken: response.access_token,
      refreshToken: response.refresh_token,
      user: response.user,
      roles: response.roles,
      scopes: response.scopes,
      permissions: response.permissions,
      windows: response.windows,
    });
  }

  private setSession(session: SessionState) {
    localStorage.setItem(ACCESS_TOKEN_KEY, session.accessToken);
    localStorage.setItem(REFRESH_TOKEN_KEY, session.refreshToken);
    // Save a profile cache so init() can restore it on network errors
    try {
      const cache = { user: session.user, roles: session.roles, scopes: session.scopes, permissions: session.permissions, windows: session.windows };
      localStorage.setItem(SESSION_CACHE_KEY, JSON.stringify(cache));
    } catch { /* storage quota — ignore */ }
    this.sessionSubject.next(session);
  }

  private clearSession() {
    localStorage.removeItem(ACCESS_TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
    localStorage.removeItem(SESSION_CACHE_KEY);
    this.sessionSubject.next(null);
  }

  private restoreSessionCache(accessToken: string, refreshToken: string): boolean {
    try {
      const raw = localStorage.getItem(SESSION_CACHE_KEY);
      if (!raw) return false;
      const cache = JSON.parse(raw) as Pick<SessionState, 'user' | 'roles' | 'scopes' | 'permissions' | 'windows'>;
      if (!cache?.user) return false;
      this.sessionSubject.next({ accessToken, refreshToken, ...cache });
      return true;
    } catch {
      return false;
    }
  }
}
