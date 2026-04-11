import { createStorageDriver } from "@mono/storage/factory";
import { StorageDriverType } from "@mono/storage";
import { env } from "../env.js";

export const storage = createStorageDriver((env.STORAGE_DRIVER as StorageDriverType) || "local", {
  bucketName: env.STORAGE_BUCKET,
  credentialsJson: env.GCP_SERVICE_ACCOUNT_KEY,
  appUrl: env.NEXT_PUBLIC_APP_URL,
});

export const importStorage = createStorageDriver(
  (env.STORAGE_DRIVER as StorageDriverType) || "local",
  {
    bucketName: env.IMPORT_BUCKET,
    credentialsJson: env.GCP_SERVICE_ACCOUNT_KEY,
    appUrl: env.NEXT_PUBLIC_APP_URL,
  },
);
