import React, { useState, useEffect } from 'react';
import { getSchema, uploadDocument, getJobStatus } from './api.js';

export default function DocumentUpload({ collections, selectedCollection, onSelectCollection }) {
  const [schema, setSchema] = useState(null);
  const [metadata, setMetadata] = useState({});
  const [file, setFile] = useState(null);
  const [error, setError] = useState('');
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!selectedCollection) {
      setSchema(null);
      return;
    }
    getSchema(selectedCollection)
      .then(s => {
        setSchema(s);
        setMetadata({});
      })
      .catch(e => setError(e.message));
  }, [selectedCollection]);

  function updateMeta(field, value) {
    setMetadata(prev => ({ ...prev, [field]: value }));
  }

  function renderInput(field) {
    const val = metadata[field.name] || '';
    switch (field.type) {
      case 'number':
        return (
          <input
            type="number"
            step="any"
            value={val}
            onChange={e => updateMeta(field.name, e.target.value)}
          />
        );
      case 'date':
        return (
          <input
            type="date"
            value={val}
            onChange={e => updateMeta(field.name, e.target.value)}
          />
        );
      case 'boolean':
        return (
          <select value={val} onChange={e => updateMeta(field.name, e.target.value)}>
            <option value="">--</option>
            <option value="true">true</option>
            <option value="false">false</option>
          </select>
        );
      default:
        return (
          <input
            type="text"
            value={val}
            onChange={e => updateMeta(field.name, e.target.value)}
          />
        );
    }
  }

  async function pollJob(jobId) {
    for (let i = 0; i < 60; i++) {
      await new Promise(r => setTimeout(r, 1000));
      try {
        const job = await getJobStatus(jobId);
        if (job.status === 'completed') {
          setStatus(`Document indexed (ID: ${job.documentId})`);
          setLoading(false);
          return;
        }
        if (job.status === 'error') {
          setError(`Indexing failed: ${job.error}`);
          setLoading(false);
          return;
        }
      } catch (e) {
        // Job endpoint may not be ready yet, keep trying
      }
    }
    setError('Job timed out');
    setLoading(false);
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setError('');
    setStatus('');

    if (!file) {
      setError('Please select a file');
      return;
    }

    setLoading(true);
    setStatus('Uploading...');

    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('metadata', JSON.stringify(metadata));

      const res = await uploadDocument(selectedCollection, formData);
      setStatus('Processing...');
      setFile(null);
      // Reset the file input
      const fileInput = document.querySelector('input[type="file"]');
      if (fileInput) fileInput.value = '';

      pollJob(res.jobId);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  }

  return (
    <div>
      <div className="collection-select">
        <label>Collection:</label>
        <select
          value={selectedCollection}
          onChange={e => onSelectCollection(e.target.value)}
        >
          {collections.length === 0 && <option value="">No collections</option>}
          {collections.map(c => (
            <option key={c.collectionName} value={c.collectionName}>
              {c.collectionName}
            </option>
          ))}
        </select>
      </div>

      {schema && (
        <div className="form-section">
          <h2>Upload Document to "{selectedCollection}"</h2>
          {error && <div className="msg error">{error}</div>}
          {status && <div className="msg info">{status}</div>}
          <form onSubmit={handleSubmit}>
            <div style={{ marginBottom: 12 }}>
              <input
                type="file"
                accept=".pdf,.docx,.xlsx,.txt,.html,.htm,.json,.csv,.xml,.md,.rtf"
                onChange={e => setFile(e.target.files[0] || null)}
              />
            </div>

            {schema.fields.map(field => (
              <div className="form-row" key={field.name}>
                <label style={{ minWidth: 140 }}>{field.name} ({field.type}):</label>
                {renderInput(field)}
              </div>
            ))}

            <div style={{ marginTop: 12 }}>
              <button type="submit" className="primary" disabled={loading}>
                {loading ? 'Processing...' : 'Upload'}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
