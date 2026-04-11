import * as React from "react";
import {
  Html,
  Head,
  Preview,
  Body,
  Container,
  Section,
  Text,
  Hr,
  Img,
  Button,
} from "@react-email/components";

export interface BookWeeklyStats {
  bookId: string;
  bookTitle: string;
  weeklyWordsAdded: number;
  streakDays: number;
  progressChartUrl: string;
  volumeChartUrl: string;
  heatmapUrl: string;
  targetHit: boolean;
}

interface WeeklyStatsEmailProps {
  userName: string;
  books: BookWeeklyStats[];
}

export const WeeklyStatsEmail = ({ userName = "Writer", books = [] }: WeeklyStatsEmailProps) => {
  return (
    <Html>
      <Head />
      <Preview>Your weekly writing stats</Preview>
      <Body style={main}>
        <Container style={container}>
          <Text style={logo}>Mono</Text>

          <Text style={greeting}>Hey {userName},</Text>
          <Text style={paragraph}>Here's a look at your writing progress over the past week.</Text>

          {books.map((book, index) => (
            <React.Fragment key={index}>
              <Text
                style={{ ...greeting, marginTop: "40px", fontWeight: "bold", color: "#2563eb" }}
              >
                {book.bookTitle}
              </Text>

              <Section style={statsBox}>
                <table
                  cellPadding={0}
                  cellSpacing={0}
                  style={{ width: "100%", textAlign: "center" }}
                >
                  <tbody>
                    <tr>
                      <td
                        style={{ width: "50%", padding: "12px", borderRight: "1px solid #e2e8f0" }}
                      >
                        <Text style={statLabel}>Words Added</Text>
                        <Text style={statValue}>{book.weeklyWordsAdded.toLocaleString()}</Text>
                      </td>
                      <td style={{ width: "50%", padding: "12px" }}>
                        <Text style={statLabel}>Current Streak</Text>
                        <Text style={statValue}>{book.streakDays} Days</Text>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </Section>

              <Hr style={hr} />

              {book.progressChartUrl && (
                <Section style={{ textAlign: "center", marginBottom: "32px" }}>
                  <Text style={{ ...statLabel, marginBottom: "16px" }}>Book Progress</Text>
                  <Img
                    src={book.progressChartUrl}
                    width="560"
                    style={chartImage}
                    alt="Progress Chart"
                  />
                </Section>
              )}

              {book.volumeChartUrl && (
                <Section style={{ textAlign: "center", marginBottom: "32px" }}>
                  <Text style={{ ...statLabel, marginBottom: "16px" }}>Daily Volume</Text>
                  <Img
                    src={book.volumeChartUrl}
                    width="560"
                    style={chartImage}
                    alt="Volume Chart"
                  />
                </Section>
              )}

              {book.heatmapUrl && (
                <Section style={{ textAlign: "center", marginBottom: "32px" }}>
                  <Text style={{ ...statLabel, marginBottom: "16px" }}>Activity Heatmap</Text>
                  <Img
                    src={book.heatmapUrl}
                    width="560"
                    style={chartImage}
                    alt="Activity Heatmap"
                  />
                </Section>
              )}

              <Section style={{ textAlign: "center", marginTop: "24px", marginBottom: "32px" }}>
                <Button style={button} href={`https://i.writemono.com/editor/${book.bookId}`}>
                  Continue Writing "{book.bookTitle}"
                </Button>
              </Section>

              {index < books.length - 1 && <Hr style={hr} />}
            </React.Fragment>
          ))}

          <Hr style={{ ...hr, marginTop: "40px" }} />

          <Text style={unsubscribe}>
            You are receiving this because you opted into Writing Coach emails.
            <br />
            You can change your preferences in your{" "}
            <a href="https://i.writemono.com/settings">Account Settings</a> at any time.
          </Text>
        </Container>
      </Body>
    </Html>
  );
};

// Styles
const main = {
  backgroundColor: "#f8fafc",
  fontFamily:
    '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Oxygen-Sans,Ubuntu,Cantarell,"Helvetica Neue",sans-serif',
};

const container = {
  margin: "0 auto",
  padding: "40px 20px",
  maxWidth: "600px",
  backgroundColor: "#ffffff",
  border: "1px solid #e2e8f0",
  borderRadius: "8px",
};

const logo = {
  fontSize: "24px",
  fontWeight: "bold",
  color: "#2563eb",
  textAlign: "center" as const,
  marginBottom: "32px",
};

const greeting = {
  fontSize: "18px",
  color: "#0f172a",
  marginBottom: "16px",
};

const paragraph = {
  fontSize: "16px",
  lineHeight: "24px",
  color: "#334155",
  marginBottom: "24px",
};

const statsBox = {
  backgroundColor: "#f1f5f9",
  borderRadius: "8px",
  marginBottom: "32px",
};

const statLabel = {
  fontSize: "12px",
  textTransform: "uppercase" as const,
  color: "#64748b",
  fontWeight: "bold",
  margin: "0 0 4px 0",
};

const statValue = {
  fontSize: "32px",
  fontWeight: "bold",
  color: "#2563eb",
  margin: "0",
};

const chartImage = {
  maxWidth: "100%",
  margin: "0 auto",
};

const hr = {
  borderColor: "#e2e8f0",
  margin: "32px 0",
};

const unsubscribe = {
  fontSize: "12px",
  color: "#94a3b8",
  textAlign: "center" as const,
};

const button = {
  backgroundColor: "#2563eb",
  borderRadius: "6px",
  color: "#fff",
  fontFamily: "inherit",
  fontSize: "16px",
  fontWeight: "bold",
  textDecoration: "none",
  textAlign: "center" as const,
  display: "inline-block",
  padding: "12px 24px",
};

export default WeeklyStatsEmail;
