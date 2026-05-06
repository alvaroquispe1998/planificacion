const fs = require('fs');
const path = 'd:\\SPIDER-DEV\\UAI\\planificacion\\backend\\src\\videoconference\\videoconference.service.ts';
let content = fs.readFileSync(path, 'utf8');

// Identify the block to replace
const startMarker = "// Use exact name and date matching (with 1-day window) to identify the correct entry among weekly sessions.";
const endMarker = "if (!match) {";

const startIndex = content.indexOf(startMarker);
const endIndex = content.indexOf(endMarker);

if (startIndex !== -1 && endIndex !== -1) {
    const newBlock = `// Use exact name and date matching (with 1-day window) to identify the correct entry among weekly sessions.
        const conferenceDate = toDateOnly(record.conference_date);
        const akademicDate = toAkademicDate(conferenceDate);

        const topicTrimmed = (topic || '').trim();
        let match =
            rows.find((r) => (r.name || '').trim() === topicTrimmed && isAkademicDateWithinDays(r.date, akademicDate, 1)) ??
            null;

        // Fallback: search by date if courseCode search didn't yield a match.
        if (!match && akademicDate) {
            const listing = await this.listAulaVirtualConferences(
                context,
                akademicDate,
                akademicDate,
                topic,
                100,
            );
            rows = listing.rows;
            match = rows.find((r) => r.name === topic) ?? null;
        }

        `;
    
    const newContent = content.slice(0, startIndex) + newBlock + content.slice(endIndex);
    fs.writeFileSync(path, newContent, 'utf8');
    console.log("Successfully updated the file using markers.");
} else {
    console.log("Could not find markers.", { startIndex, endIndex });
}
