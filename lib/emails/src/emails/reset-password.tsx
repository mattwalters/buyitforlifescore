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

interface ResetPasswordProps {
  url?: string;
  host?: string;
}

export const ResetPassword = ({
  url = "http://localhost:3000/auth/new-password?token=xxx",
  host = "Mono",
}: ResetPasswordProps) => {
  return (
    <Html>
      <Head />
      <Preview>Reset your password for {host}</Preview>
      <Tailwind>
        <Body className="bg-white my-auto mx-auto font-sans">
          <Container className="border border-solid border-[#eaeaea] rounded my-[40px] mx-auto p-[20px] max-w-[465px]">
            <Heading className="text-black text-[24px] font-normal text-center p-0 my-[30px] mx-0">
              Reset your password
            </Heading>
            <Text className="text-black text-[14px] leading-[24px]">Hello,</Text>
            <Text className="text-black text-[14px] leading-[24px]">
              Someone requested that the password for your account on <strong>{host}</strong> be
              reset.
            </Text>
            <Section className="text-center mt-[32px] mb-[32px]">
              <Button
                className="bg-[#000000] rounded text-white text-[12px] font-semibold no-underline text-center px-5 py-3"
                href={url}
              >
                Reset Password
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

export default ResetPassword;
