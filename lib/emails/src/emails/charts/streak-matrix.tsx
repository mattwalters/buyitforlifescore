import * as React from "react";

interface StreakMatrixProps {
  // A flat array of 28 numbers representing intensity from 0 to 3
  // 0: none, 1: low, 2: medium, 3: high
  intensities: number[];
}

export const StreakMatrix = ({ intensities }: StreakMatrixProps) => {
  // We want to render a grid of 4 columns representing weeks, and 7 rows representing days of week.
  // Wait, standard GitHub matrix is columns = weeks, rows = days (Sun-Sat).
  // intensities should be ordered chronologically. We'll chunk them manually.

  const bgColors = {
    0: "#f1f5f9", // slate-100
    1: "#bbf7d0", // green-200
    2: "#4ade80", // green-400
    3: "#22c55e", // green-500
  };

  return (
    <div style={{ margin: "20px 0", textAlign: "center" as const }}>
      <h3 style={{ margin: "0 0 10px 0", fontSize: "16px", color: "#334155" }}>The Chain</h3>
      <table
        cellPadding={0}
        cellSpacing={0}
        style={{
          margin: "0 auto",
          borderCollapse: "separate",
          borderSpacing: "4px",
        }}
      >
        <tbody>
          {[0, 1, 2, 3, 4, 5, 6].map((dayOfWeek) => (
            <tr key={dayOfWeek}>
              {[0, 1, 2, 3].map((week) => {
                const index = week * 7 + dayOfWeek;
                const val = intensities[index] || 0;
                const bg = bgColors[val as keyof typeof bgColors];
                return (
                  <td
                    key={week}
                    style={{
                      width: "22px",
                      height: "22px",
                      backgroundColor: bg,
                      borderRadius: "4px",
                    }}
                  />
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ marginTop: "12px", fontSize: "12px", color: "#64748b" }}>
        Don't break the streak!
      </div>
    </div>
  );
};
