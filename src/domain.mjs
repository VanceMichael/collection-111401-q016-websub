import { capture } from './client.mjs';
export { createInbox, Inbox, InboxError } from './inbox.mjs';
export async function receiveCurrent(base, directory) {
 const { bytes, receipt } = await capture(base + '/publisher', directory);
 const data = JSON.parse(bytes);
 return { receipt: receipt.id, publisher: data.publisher, topic: data.topic, hub: data.hub };
}
