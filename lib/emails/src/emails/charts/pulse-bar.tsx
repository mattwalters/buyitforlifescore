import * as React from "react";

interface PulseBarProps {
  // Array of 7 objects representing the last 7 days of word counts
  data: { dayName: string; words: number }[];
  targetPace: number;
}

export const PulseBar = ({ data, targetPace }: PulseBarProps) => {
  const maxVal = Math.max(...data.map((d) => d.words), targetPace * 1.2, 500);

  return (
    <div style={{ margin: "20px 0", textAlign: "center" as const }}>
      <h3 style={{ margin: "0 0 10px 0", fontSize: "16px", color: "#334155" }}>Weekly Pulse</h3>
      <table
        cellPadding={0}
        cellSpacing={0}
        style={{
          width: "100%",
          maxWidth: "300px",
          margin: "0 auto",
          borderCollapse: "collapse",
          height: "150px",
        }}
      >
        <tbody>
          <tr style={{ verticalAlign: "bottom" }}>
            {data.map((d, i) => {
              const heightPct = Math.max(2, Math.min(100, Math.round((d.words / maxVal) * 100)));

              // We'll draw a target line using absolute positioning or just draw the bars
              return (
                <td
                  key={i}
                  style={{
                    width: "14%",
                    textAlign: "center",
                    padding: "0 4px",
                    position: "relative",
                  }}
                >
                  <div
                    style={{
                      backgroundColor: "#3b82f6",
                      height: `${heightPct}%`,
                      borderTopLeftRadius: "4px",
                      borderTopRightRadius: "4px",
                      width: "100%",
                    }}
                  />
                </td>
              );
            })}
          </tr>
        </tbody>
      </table>
      {/* Target line approximation below */}
      <div
        style={{
          width: "100%",
          maxWidth: "300px",
          margin: "0 auto",
          borderTop: "2px dashed #ef4444",
          position: "relative",
          top: `-${Math.round((targetPace / maxVal) * 150)}px`,
          height: "0",
        }}
      />

      {/* Axis Labels */}
      <table
        cellPadding={0}
        cellSpacing={0}
        style={{
          width: "100%",
          maxWidth: "300px",
          margin: "8px auto 0",
          borderCollapse: "collapse",
        }}
      >
        <tbody>
          <tr>
            {data.map((d, i) => (
              <td
                key={i}
                style={{ width: "14%", textAlign: "center", fontSize: "10px", color: "#64748b" }}
              >
                {d.dayName}
              </td>
            ))}
          </tr>
        </tbody>
      </table>
    </div>
  );
};
