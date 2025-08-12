import type { Metadata } from "next";
import {
  ClerkProvider,
  SignInButton,
  SignUpButton,
  SignedIn,
  SignedOut,
  UserButton,
} from "@clerk/nextjs";
import "./globals.css";

export const metadata: Metadata = {
  title: "Disclosure Review System - Conflixis",
  description: "Healthcare Provider Disclosure Policy Review and Compliance System",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <ClerkProvider>
      <html lang="en">
        <body className="antialiased">
          <header className="absolute top-4 right-4 z-50">
            <SignedOut>
              <div className="flex gap-2">
                <SignInButton mode="modal">
                  <button className="px-4 py-2 text-sm bg-conflixis-green text-white rounded-lg hover:bg-opacity-90">
                    Sign In
                  </button>
                </SignInButton>
                <SignUpButton mode="modal">
                  <button className="px-4 py-2 text-sm border border-conflixis-green text-conflixis-green rounded-lg hover:bg-conflixis-green hover:text-white">
                    Sign Up
                  </button>
                </SignUpButton>
              </div>
            </SignedOut>
            <SignedIn>
              <UserButton afterSignOutUrl="/" />
            </SignedIn>
          </header>
          {children}
        </body>
      </html>
    </ClerkProvider>
  );
}