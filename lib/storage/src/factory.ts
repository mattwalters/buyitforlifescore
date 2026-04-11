import { StorageDriver, StorageDriverType } from "./index.js";
import { LocalStorageDriver } from "./drivers/local.js";
import { GCSStorageDriver } from "./drivers/gcs.js";

export interface StorageDriverConfig {
  bucketName?: string;
  credentialsJson?: string;
  appUrl: string;
}

export function createStorageDriver(
  type: StorageDriverType,
  config: StorageDriverConfig,
): StorageDriver {
  switch (type) {
    case "gcs": {
      if (!config.bucketName) {
        throw new Error("bucketName configuration is required for GCS driver");
      }

      let credentials;
      if (config.credentialsJson) {
        try {
          credentials = JSON.parse(config.credentialsJson);
        } catch (e) {
          console.error("Failed to parse GCP credentials json", e);
        }
      }

      return new GCSStorageDriver(config.bucketName, { credentials });
    }
    case "local":
      return new LocalStorageDriver(config.appUrl);
    default:
      console.warn(`Unknown storage driver "${type}", falling back to local`);
      return new LocalStorageDriver(config.appUrl);
  }
}
