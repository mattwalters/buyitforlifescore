export interface StorageDriver {
  /**
   * Get a presigned URL for uploading a file.
   * @param path The path where the file should be stored (e.g. "uploads/xyz.png")
   * @param contentType The MIME type of the file
   * @returns An object containing the URL and any necessary method/headers
   */
  getPresignedPutUrl(path: string, contentType: string): Promise<{ url: string; method: string }>;

  /**
   * Get a public URL for a file.
   * @param path The path where the file is stored
   */
  getPublicUrl(path: string): string;

  /**
   * Delete a file.
   * @param path The path of the file to delete
   */
  delete(path: string): Promise<void>;

  /**
   * Download a file.
   * @param path The path of the file to download
   * @returns A Buffer containing the file data
   */
  download(path: string): Promise<Buffer>;
}

export type StorageDriverType = "local" | "gcs";
