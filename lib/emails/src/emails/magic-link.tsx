import {
  Body,
  Button,
  Container,
  Head,
  Heading,
  Html,
  Preview,
  Section,
  Text,
  Tailwind,
} from "@react-email/components";
import * as React from "react";

interface MagicLinkProps {
  url?: string;
  host?: string;
}

export const MagicLink = ({
  url = "http://localhost:3000/api/auth/magic-link?token=xxx",
  host = "Mono",
}: MagicLinkProps) => {
  return (
    <Html>
      <Head />
      <Preview>Sign in to {host}</Preview>
      <Tailwind>
        <Body className="bg-white my-auto mx-auto font-sans">
          <Container className="border border-solid border-[#eaeaea] rounded my-[40px] mx-auto p-[20px] max-w-[465px]">
            <Heading className="text-black text-[24px] font-normal text-center p-0 my-[30px] mx-0">
              Sign in to {host}
            </Heading>
            <Text className="text-black text-[14px] leading-[24px]">Hello,</Text>
            <Text className="text-black text-[14px] leading-[24px]">
              Click the button below to sign in to <strong>{host}</strong>.
            </Text>
            <Section className="text-center mt-[32px] mb-[32px]">
              <Button
                className="bg-[#000000] rounded text-white text-[12px] font-semibold no-underline text-center px-5 py-3"
                href={url}
              >
                Sign In
              </Button>
            </Section>
            <Text className="text-black text-[14px] leading-[24px]">
              If you didn't request this, you can ignore this email.
            </Text>
          </Container>
        </Body>
      </Tailwind>
    </Html>
  );
};

export default MagicLink;
