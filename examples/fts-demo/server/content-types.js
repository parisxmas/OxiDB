const EXTENSION_MAP = {
  '.pdf':  'application/pdf',
  '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.txt':  'text/plain',
  '.html': 'text/html',
  '.htm':  'text/html',
  '.json': 'application/json',
  '.csv':  'text/csv',
  '.xml':  'application/xml',
  '.md':   'text/markdown',
  '.rtf':  'application/rtf',
};

const ALLOWED_EXTENSIONS = new Set(Object.keys(EXTENSION_MAP));

function getContentType(filename) {
  const ext = filename.slice(filename.lastIndexOf('.')).toLowerCase();
  return EXTENSION_MAP[ext] || 'application/octet-stream';
}

function isAllowed(filename) {
  const ext = filename.slice(filename.lastIndexOf('.')).toLowerCase();
  return ALLOWED_EXTENSIONS.has(ext);
}

module.exports = { getContentType, isAllowed, EXTENSION_MAP };
