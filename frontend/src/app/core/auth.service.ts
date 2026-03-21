import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
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
  throwError,
} from 'rxjs';
import { AuthResponse, PermissionCode, SessionState, WindowCode } from './auth.models';
import { firstAllowedPath } from './navigation';

const ACCESS_TOKEN_KEY = 'uai.auth.access_token';
const REFRESH_TOKEN_KEY = 'uai.auth.refresh_token';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly baseUrl = `${environment.apiBaseUrl}/auth`;
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
    } catch {
      try {
        await firstValueFrom(this.refreshAccessToken());
      } catch {
        this.clearSession();
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
    return this.http.get<Omit<AuthResponse, 'access_token' | 'refresh_token'>>(`${this.baseUrl}/me`).pipe(
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
    return this.router.navigateByUrl(this.firstAllowedPath());
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
    this.sessionSubject.next(session);
  }

  private clearSession() {
    localStorage.removeItem(ACCESS_TOKEN_KEY);
    localStorage.removeItem(REFRESH_TOKEN_KEY);
    this.sessionSubject.next(null);
  }
}
