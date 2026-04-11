import { StorageDriver } from "../index.js";
import { Storage, type StorageOptions } from "@google-cloud/storage";

export class GCSStorageDriver implements StorageDriver {
  private storage: Storage;
  private bucketName: string;

  constructor(bucketName: string, options?: StorageOptions) {
    this.storage = new Storage(options);
    this.bucketName = bucketName;
  }

  async getPresignedPutUrl(path: string, contentType: string) {
    const [url] = await this.storage
      .bucket(this.bucketName)
      .file(path)
      .getSignedUrl({
        version: "v4",
        action: "write",
        expires: Date.now() + 15 * 60 * 1000, // 15 minutes
        contentType,
      });

    return {
      url,
      method: "PUT",
    };
  }

  getPublicUrl(path: string) {
    // Assuming the bucket is public or we use the public storage API link.
    // "https://storage.googleapis.com/BUCKET_NAME/PATH"
    // encodeURIComponent on path parts might be needed.
    return `https://storage.googleapis.com/${this.bucketName}/${path}`;
  }

  async delete(path: string) {
    await this.storage.bucket(this.bucketName).file(path).delete();
  }

  async download(path: string): Promise<Buffer> {
    const [buffer] = await this.storage.bucket(this.bucketName).file(path).download();
    return buffer;
  }
}
