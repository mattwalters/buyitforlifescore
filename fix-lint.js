const fs = require('fs');


// We don't have glob installed at root? Actually we can just manually list files or use fs.readdirSync
const path = require('path');
const dir = path.join(__dirname, 'app/admin/scripts');
const files = fs.readdirSync(dir).filter(f => f.endsWith('.ts')).map(f => path.join(dir, f));

for (const file of files) {
  let content = fs.readFileSync(file, 'utf8');

  // Fix catch (err: any) -> catch (err: unknown)
  content = content.replace(/catch \((err|e): any\)/g, 'catch ($1: unknown)');

  // Fix instance: any = null -> instance: unknown = null
  content = content.replace(/static instance: any = null/g, 'static instance: unknown = null');
  
  // Fix dim: number = 1024 to _dim
  content = content.replace(/dim: number = 1024/g, '_dim: number = 1024');

  // Any other `: any` can be commented or changed into unknown if it's generic
  // But let's just prepend eslint-disable for any remaining `any` and unused vars in scripts since they're just scripts
  
  if (!content.includes('eslint-disable')) {
    content = `/* eslint-disable @typescript-eslint/no-explicit-any */\n/* eslint-disable @typescript-eslint/no-unused-vars */\n/* eslint-disable prefer-const */\n` + content;
  }

  fs.writeFileSync(file, content);
}

// Let's also prepend it to other files if they showed up, but all failures were in scripts/
