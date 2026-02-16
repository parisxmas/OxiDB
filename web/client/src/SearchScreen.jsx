import React, { useState, useEffect } from 'react';
import { getSchema, searchDocuments } from './api.js';

export default function SearchScreen({ collections, selectedCollection, onSelectCollection }) {
  const [schema, setSchema] = useState(null);
  const [filters, setFilters] = useState({});
  const [textQuery, setTextQuery] = useState('');
  const [results, setResults] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [skip, setSkip] = useState(0);
  const limit = 20;

  useEffect(() => {
    if (!selectedCollection) {
      setSchema(null);
      return;
    }
    getSchema(selectedCollection)
      .then(s => {
        setSchema(s);
        setFilters({});
        setResults(null);
      })
      .catch(e => setError(e.message));
  }, [selectedCollection]);

  function updateFilter(field, key, value) {
    setFilters(prev => ({
      ...prev,
      [field]: { ...prev[field], [key]: value },
    }));
  }

  function renderFilter(field) {
    if (!field.indexed) return null;

    switch (field.type) {
      case 'number':
        return (
          <div className="filter-group" key={field.name}>
            <label>{field.name} (range)</label>
            <div className="range-inputs">
              <input
                type="number"
                step="any"
                placeholder="Min"
                value={filters[field.name]?.min ?? ''}
                onChange={e => updateFilter(field.name, 'min', e.target.value === '' ? null : Number(e.target.value))}
              />
              <input
                type="number"
                step="any"
                placeholder="Max"
                value={filters[field.name]?.max ?? ''}
                onChange={e => updateFilter(field.name, 'max', e.target.value === '' ? null : Number(e.target.value))}
              />
            </div>
          </div>
        );

      case 'date':
        return (
          <div className="filter-group" key={field.name}>
            <label>{field.name} (range)</label>
            <div className="range-inputs">
              <input
                type="date"
                value={filters[field.name]?.min ?? ''}
                onChange={e => updateFilter(field.name, 'min', e.target.value || null)}
              />
              <input
                type="date"
                value={filters[field.name]?.max ?? ''}
                onChange={e => updateFilter(field.name, 'max', e.target.value || null)}
              />
            </div>
          </div>
        );

      case 'boolean':
        return (
          <div className="filter-group" key={field.name}>
            <label>{field.name}</label>
            <select
              value={filters[field.name]?.value ?? ''}
              onChange={e => updateFilter(field.name, 'value', e.target.value === '' ? null : e.target.value === 'true')}
            >
              <option value="">Any</option>
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          </div>
        );

      default:
        return (
          <div className="filter-group" key={field.name}>
            <label>{field.name}</label>
            <input
              type="text"
              placeholder={`Filter by ${field.name}`}
              value={filters[field.name]?.value ?? ''}
              onChange={e => updateFilter(field.name, 'value', e.target.value || null)}
            />
          </div>
        );
    }
  }

  // Strip out empty filters before sending
  function cleanFilters() {
    const cleaned = {};
    for (const [key, val] of Object.entries(filters)) {
      if (!val) continue;
      if (val.min != null || val.max != null) {
        cleaned[key] = val;
      } else if (val.value != null && val.value !== '') {
        cleaned[key] = val;
      }
    }
    return cleaned;
  }

  async function handleSearch(newSkip = 0) {
    setError('');
    setLoading(true);
    setSkip(newSkip);

    try {
      const res = await searchDocuments(
        selectedCollection,
        cleanFilters(),
        textQuery.trim(),
        newSkip,
        limit
      );
      setResults(res);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  function getDisplayValue(doc, fieldName) {
    const parts = fieldName.split('.');
    let val = doc;
    for (const p of parts) {
      if (val == null) return '';
      val = val[p];
    }
    if (val == null) return '';
    if (typeof val === 'object') return JSON.stringify(val);
    return String(val);
  }

  const indexedFields = schema?.fields.filter(f => f.indexed) || [];
  const allFields = schema?.fields || [];

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
          <h2>Search "{selectedCollection}"</h2>
          {error && <div className="msg error">{error}</div>}

          <div className="fts-input">
            <input
              type="text"
              placeholder="Full-text search (searches file contents)..."
              value={textQuery}
              onChange={e => setTextQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
            />
          </div>

          {indexedFields.length > 0 && (
            <>
              <div style={{ fontSize: '0.85rem', fontWeight: 600, marginBottom: 8, color: '#555' }}>
                Field Filters
              </div>
              <div className="filter-grid">
                {indexedFields.map(f => renderFilter(f))}
              </div>
            </>
          )}

          <button className="primary" onClick={() => handleSearch()} disabled={loading}>
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      )}

      {results && (
        <div className="form-section">
          <h2>Results ({results.total ?? results.docs.length})</h2>
          {results.docs.length === 0 ? (
            <p style={{ color: '#999', fontSize: '0.9rem' }}>No documents found</p>
          ) : (
            <>
              <table className="results-table">
                <thead>
                  <tr>
                    <th>_id</th>
                    {allFields.map(f => <th key={f.name}>{f.name}</th>)}
                    <th>File</th>
                    {results.mode !== 'structured' && results.mode !== 'all' && <th>_score</th>}
                  </tr>
                </thead>
                <tbody>
                  {results.docs.map((doc, i) => (
                    <tr key={doc._id || i}>
                      <td>{doc._id}</td>
                      {allFields.map(f => (
                        <td key={f.name} title={getDisplayValue(doc, f.name)}>
                          {getDisplayValue(doc, f.name)}
                        </td>
                      ))}
                      <td title={doc._file?.originalName}>
                        {doc._file ? (
                          <a
                            href={`/api/collections/${encodeURIComponent(selectedCollection)}/files/${encodeURIComponent(doc._file.key)}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {doc._file.originalName}
                          </a>
                        ) : '—'}
                      </td>
                      {results.mode !== 'structured' && results.mode !== 'all' && (
                        <td>{doc._score != null ? doc._score.toFixed(2) : '—'}</td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="pagination">
                <button
                  className="secondary"
                  disabled={skip === 0}
                  onClick={() => handleSearch(Math.max(0, skip - limit))}
                >
                  Previous
                </button>
                <span>Showing {skip + 1}–{skip + results.docs.length}</span>
                <button
                  className="secondary"
                  disabled={results.docs.length < limit}
                  onClick={() => handleSearch(skip + limit)}
                >
                  Next
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
