export const CLIENT_BUILD_ID = import.meta.env.VITE_FRONTEND_BUILD_ID ?? 'dev';
export const CLIENT_APP_VERSION = import.meta.env.VITE_APP_VERSION ?? '0.1.0';
export const CLIENT_API_SCHEMA_VERSION = Number(import.meta.env.VITE_API_SCHEMA_VERSION ?? '1');

export interface VersionInfo {
  app_version: string;
  frontend_build_id: string;
  api_schema_version: number;
  started_at: string;
}

export const CLIENT_IDENTITY = {
  app_version: CLIENT_APP_VERSION,
  frontend_build_id: CLIENT_BUILD_ID,
  api_schema_version: CLIENT_API_SCHEMA_VERSION,
};
