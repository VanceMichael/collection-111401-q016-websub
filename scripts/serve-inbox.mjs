import { createInbox } from '../src/inbox.mjs';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const directory = process.env.INBOX_DIR ?? await mkdtemp(join(tmpdir(), 'collection-inbox-'));
const inbox = await createInbox({ directory, port: Number(process.env.PORT ?? 8081) });
console.log('收件服务：' + inbox.base + ' 数据目录：' + directory);
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await inbox.close(); process.exit(0); });
