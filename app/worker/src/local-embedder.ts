import { pipeline, env } from "@xenova/transformers";

// Optional: Don't load local remote models if not found, pull straight from HuggingFace
env.allowLocalModels = false;

class EmbedderPipeline {
  static task = "feature-extraction" as const;
  static model = "mixedbread-ai/mxbai-embed-large-v1";
  static instance: unknown = null;

  static async getInstance() {
    if (this.instance === null) {
      console.log(`[ONNX] Booting local embedding engine: ${this.model}...`);
      this.instance = await pipeline(this.task, this.model);
      console.log(`[ONNX] Engine online and loaded into memory!`);
    }
    return this.instance;
  }
}

export const embedWithRetry = async (text: string, _dim: number = 1024): Promise<number[]> => {
  try {
    const extractor = await EmbedderPipeline.getInstance();
    // Use format specific to Mxbai: pooling="cls", normalize=true
    const output = await extractor(text, { pooling: "cls", normalize: true });
    return Array.from(output.data);
  } catch (err: unknown) {
    console.error(`[ONNX] Embedding failed:`, (err as Error).message);
    return [];
  }
};
