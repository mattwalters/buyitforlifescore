"use server";

import { signIn, signOut } from "@/auth";
import { AuthError } from "next-auth";

export async function handleSignOut() {
  await signOut();
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function authenticate(_prevState: any, formData: FormData) {
  try {
    await signIn("credentials", formData);
  } catch (error) {
    if (error instanceof AuthError) {
      switch (error.type) {
        case "CredentialsSignin":
          return "Invalid credentials.";
        default:
          return "Something went wrong.";
      }
    }
    throw error;
  }
}
