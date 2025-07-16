#!/usr/bin/env node

import * as fs from 'fs/promises';
import * as path from 'path';
import { buildDocumentation, documentationToMarkdown } from 'tsdoc-markdown';
import { fileURLToPath } from 'url';

// @ts-ignore
if (typeof __dirname === 'undefined') global.__dirname = (typeof import.meta === 'undefined') ? process.cwd() : path.dirname(fileURLToPath(import.meta.url));

const projectRoot = path.resolve(__dirname, '..');

/**
* Generate markdown documentation for a TypeScript file using tsdoc-markdown
* @param {string} filePath Path to the TypeScript file
* @returns {Promise<string>} Generated markdown documentation
*/
async function generateMarkdownDoc(filePath) {
    let entries = buildDocumentation({
        inputFiles: [filePath],
        options: {}
    });

    entries = entries.filter((doc) => {
        if (doc.doc_type === 'const') doc.doc_type = 'function';
        return doc.doc_type === 'function';
    });

    const markdownContent = documentationToMarkdown({entries, options: {
        emoji: null,
        headingLevel: "##",
    }});
    
    // Remove headers and TOCs
    return markdownContent.replace(/^##[\s\S]*?\n###/g, '###').trim();
}

/**
* Update README.md with generated TSDoc content
*/
async function updateReadme() {
    const readmePath = path.join(projectRoot, 'README.md');
    const readme = await fs.readFile(readmePath, 'utf8');
    
    // Find the marker
    const markerRegex = /(The following is auto-generated from `([^`]+)`:\s*\n)([\s\S]*?)(?=\n## |$)/g;
    let match;
    let updatedReadme = readme;
    let filesProcessed = 0;
    
    // Process each marker found in the README
    while ((match = markerRegex.exec(readme)) !== null) {
        const [fullMatch, startLine, sourceFile, oldContent] = match;
        const absoluteSourcePath = path.resolve(projectRoot, sourceFile);
        
        console.log(`Generating docs for ${sourceFile}...`);
        const newContent = await generateMarkdownDoc(absoluteSourcePath);
        
        // Replace the old content with new content
        updatedReadme = updatedReadme.replace(fullMatch,startLine+newContent);
        
        filesProcessed++;
    }
    
    if (filesProcessed === 0) {
        console.error('Could not find any auto-generated markers in README.md');
        process.exit(1);
    }
    
    await fs.writeFile(readmePath, updatedReadme);
    console.log(`Updated documentation for ${filesProcessed} file(s) in README.md`);
}

updateReadme().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
