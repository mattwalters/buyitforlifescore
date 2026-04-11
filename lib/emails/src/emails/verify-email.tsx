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

interface VerifyEmailProps {
  url?: string;
  host?: string;
}

export const VerifyEmail = ({
  url = "http://localhost:3000/api/auth/callback/email",
  host = "Mono",
}: VerifyEmailProps) => {
  return (
    <Html>
      <Head />
      <Preview>Verify your email address for {host}</Preview>
      <Tailwind>
        <Body className="bg-white my-auto mx-auto font-sans">
          <Container className="border border-solid border-[#eaeaea] rounded my-[40px] mx-auto p-[20px] max-w-[465px]">
            <Heading className="text-black text-[24px] font-normal text-center p-0 my-[30px] mx-0">
              Verify your email address
            </Heading>
            <Text className="text-black text-[14px] leading-[24px]">Hello,</Text>
            <Text className="text-black text-[14px] leading-[24px]">
              Click the button below to verify your email address and sign in to{" "}
              <strong>{host}</strong>.
            </Text>
            <Section className="text-center mt-[32px] mb-[32px]">
              <Button
                className="bg-[#000000] rounded text-white text-[12px] font-semibold no-underline text-center px-5 py-3"
                href={url}
              >
                Verify Email
              </Button>
            </Section>
            <Text className="text-black text-[14px] leading-[24px]">
              or copy and paste this URL into your browser:{" "}
              <a href={url} className="text-blue-600 no-underline">
                {url}
              </a>
            </Text>
          </Container>
        </Body>
      </Tailwind>
    </Html>
  );
};

export default VerifyEmail;
