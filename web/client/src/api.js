const BASE = '/api';

async function request(path, options = {}) {
  const fetchOpts = { ...options };
  if (!(fetchOpts.body instanceof FormData)) {
    fetchOpts.headers = { 'Content-Type': 'application/json', ...fetchOpts.headers };
  }
  const res = await fetch(`${BASE}${path}`, fetchOpts);
  if (!res.ok) {
    let msg = `Request failed: ${res.status}`;
    try {
      const data = await res.json();
      if (data.error) msg = data.error;
    } catch (e) { /* not json */ }
    throw new Error(msg);
  }
  return res.json();
}

export function listCollections() {
  return request('/collections');
}

export function createCollection(collectionName, fields) {
  return request('/collections', {
    method: 'POST',
    body: JSON.stringify({ collectionName, fields }),
  });
}

export function deleteCollection(name) {
  return request(`/collections/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
}

export function getSchema(name) {
  return request(`/collections/${encodeURIComponent(name)}/schema`);
}

export function uploadDocument(collection, formData) {
  return request(`/collections/${encodeURIComponent(collection)}/documents`, {
    method: 'POST',
    body: formData,
  });
}

export function listDocuments(collection, skip = 0, limit = 20) {
  return request(`/collections/${encodeURIComponent(collection)}/documents?skip=${skip}&limit=${limit}`);
}

export function getJobStatus(jobId) {
  return request(`/jobs/${encodeURIComponent(jobId)}`);
}

export function searchDocuments(collection, filters, textQuery, skip = 0, limit = 20) {
  return request(`/collections/${encodeURIComponent(collection)}/search`, {
    method: 'POST',
    body: JSON.stringify({ filters, textQuery, skip, limit }),
  });
}
