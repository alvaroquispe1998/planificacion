import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, switchMap, throwError } from 'rxjs';
import { AuthService } from './auth.service';

export const authInterceptor: HttpInterceptorFn = (request, next) => {
  const auth = inject(AuthService);
  const router = inject(Router);

  const isAuthRefresh = request.url.endsWith('/auth/refresh');
  const isAuthLogin = request.url.endsWith('/auth/login');
  const accessToken = auth.accessToken;

  const authorizedRequest =
    accessToken && !isAuthLogin && !isAuthRefresh
      ? request.clone({
          setHeaders: {
            Authorization: `Bearer ${accessToken}`,
          },
        })
      : request;

  return next(authorizedRequest).pipe(
    catchError((error: HttpErrorResponse) => {
      if (error.status !== 401 || isAuthLogin || isAuthRefresh || !auth.refreshToken) {
        return throwError(() => error);
      }

      return auth.refreshAccessToken().pipe(
        switchMap((token) => {
          if (!token) {
            void router.navigateByUrl('/login');
            return throwError(() => error);
          }
          return next(
            request.clone({
              setHeaders: {
                Authorization: `Bearer ${token}`,
              },
            }),
          );
        }),
        catchError((refreshError) => {
          void router.navigateByUrl('/login');
          return throwError(() => refreshError);
        }),
      );
    }),
  );
};
