const fs = require('fs');
const path = 'd:\\SPIDER-DEV\\UAI\\planificacion\\backend\\src\\videoconference\\videoconference.service.ts';
let content = fs.readFileSync(path, 'utf8');

const target = `            const key = \`\${(section.course_id ?? '').trim()}::\${(section.name ?? '').trim()}\`;
            out.set(id, replacement?.id ?? id);
        }`;

const replacement = `            const key = \`\${(section.course_id ?? '').trim()}::\${(section.name ?? '').trim()}\`;
            const best = bestCandidateByPair.get(key);
            out.set(id, best?.id ?? id);
        }`;

if (content.includes(target)) {
    content = content.replace(target, replacement);
    fs.writeFileSync(path, content, 'utf8');
    console.log("Successfully fixed resolveEffectiveVcSectionIds.");
} else {
    console.log("Could not find target string.");
    // Try a more lenient search
    const lines = content.split('\n');
    for (let i = 0; i < lines.length; i++) {
        if (lines[i].includes('out.set(id, replacement?.id ?? id);')) {
            console.log("Found faulty line at " + (i + 1));
            lines[i] = '            const best = bestCandidateByPair.get(key);';
            lines.splice(i + 1, 0, '            out.set(id, best?.id ?? id);');
            fs.writeFileSync(path, lines.join('\n'), 'utf8');
            console.log("Fixed faulty line.");
            break;
        }
    }
}
