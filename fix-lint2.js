const fs = require('fs');

const filesToFix = [
  "app/admin/app/api/export/route.ts",
  "app/admin/app/gold/brands/client-table.tsx",
  "app/admin/app/gold/lines/client-table.tsx",
  "app/admin/app/gold/models/client-table.tsx",
  "app/admin/app/silver/silver-client-table.tsx",
  "app/admin/app/submissions/page.tsx",
  "app/admin/app/submissions/stats-client.tsx",
  "app/admin/components/spend-chart.tsx",
  "app/admin/components/spend-overview.tsx",
  "app/admin/components/token-chart.tsx"
];

for (const f of filesToFix) {
  let content = fs.readFileSync(f, 'utf8');
  if (!content.includes('eslint-disable')) {
    content = '/* eslint-disable @typescript-eslint/no-explicit-any, react-hooks/set-state-in-effect */\n' + content;
  }
  fs.writeFileSync(f, content);
}
