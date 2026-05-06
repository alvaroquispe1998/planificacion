const fs = require('fs');
const path = 'd:\\SPIDER-DEV\\UAI\\planificacion\\backend\\src\\videoconference\\videoconference.service.ts';
let content = fs.readFileSync(path, 'utf8');

const target = "audit_sync_error: ownerRecord?.audit_sync_error ?? null,";
const replacement = `audit_sync_error: ownerRecord?.audit_sync_error ?? null,
                delete_status: null,
                deleted_at: null,
                deleted_by: null,
                delete_error: null,
                zoom_deleted_at: null,
                akademic_deleted_at: null,`;

if (content.includes(target)) {
    // Only replace the one inside upsertInheritedOccurrence (around line 4282)
    // Actually, it's safer to just replace all occurrences if it's correct.
    // In this file, this pattern is likely only in those merge/create blocks.
    content = content.replace(target, replacement);
    fs.writeFileSync(path, content, 'utf8');
    console.log("Successfully updated the file using string replacement.");
} else {
    console.log("Could not find target string.");
}
