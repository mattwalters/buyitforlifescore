import { StorageDriver } from "../index.js";

export class LocalStorageDriver implements StorageDriver {
  constructor(private readonly baseUrl: string = "http://localhost:3000") {}

  async getPresignedPutUrl(path: string, _contentType: string) {
    // For local, we return a URL to our own API route.
    // The path is encoded as a query param or part of the URL.
    // We'll use a query param for simplicity in the API route.
    const url = new URL(`${this.baseUrl}/api/uploads/local`);
    url.searchParams.set("path", path);
    // We don't really need to sign it for local dev, but we could HMAC it if we cared.

    return {
      url: url.toString(),
      method: "PUT",
    };
  }

  getPublicUrl(path: string) {
    // Local uploads are stored in public/uploads, so they are served relative to root.
    // Ensure path doesn't start with / to avoid double slashes if we prepend something.
    const cleanPath = path.startsWith("/") ? path.slice(1) : path;
    return `${this.baseUrl}/uploads/${cleanPath}`;
  }

  async delete(path: string) {
    // In a real local driver, we need to delete the file from public/uploads.
    // We assume the path passed here is relative to public/uploads (e.g. "path/to/image.png")
    // because that's how we store it in the database and how getPublicUrl expects it.

    try {
      // We need to resolve the path.
      // In Next.js, process.cwd() usually points to the root of the project (or workspace).
      // Since we are likely running in the web app context (next start/dev),
      // and the upload route used `join(process.cwd(), "public", "uploads", path)`,
      // we should do the same here.

      // Note: This assumes this code is running in the Next.js server context.
      // If run from a worker, this might fail if the worker doesn't have access to the same FS.
      // But for now, user uploads/deletes happen in the web app.

      const fs = await import("fs/promises");
      const { join } = await import("path");

      // Sanitize path to prevent traversal (though expected path is from DB)
      if (path.includes("..") || path.startsWith("/")) {
        // It might start with / if it was stored that way, but let's be careful.
        // The upload route stores it *relative* to uploads/
      }

      const cleanPath = path.replace(/^\/+/, ""); // Remove leading slashes

      let cwd = process.cwd();
      if (cwd.match(/app[\\/]worker$/)) {
        cwd = join(cwd, "..", "web");
      }

      const fullPath = join(cwd, "public", "uploads", cleanPath);

      console.log(`[LocalDriver] Deleting file at ${fullPath}`);
      await fs.unlink(fullPath);
    } catch (error: unknown) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        console.warn(`[LocalDriver] File not found for deletion: ${path}`);
        return;
      }
      console.error(`[LocalDriver] Failed to delete file: ${path}`, error);
      throw error;
    }
  }

  async download(path: string): Promise<Buffer> {
    const fs = await import("fs/promises");
    const { join } = await import("path");
    const cleanPath = path.replace(/^\/+/, "");

    let cwd = process.cwd();
    if (cwd.match(/app[\\/]worker$/)) {
      cwd = join(cwd, "..", "web");
    }

    const fullPath = join(cwd, "public", "uploads", cleanPath);
    console.log(`[LocalDriver] Downloading file from ${fullPath}`);
    return fs.readFile(fullPath);
  }
}
