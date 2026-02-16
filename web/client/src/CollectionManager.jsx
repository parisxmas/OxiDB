import React, { useState } from 'react';
import { createCollection, deleteCollection } from './api.js';

const FIELD_TYPES = ['string', 'number', 'date', 'boolean'];

const emptyField = () => ({ name: '', type: 'string', indexed: false });

export default function CollectionManager({ collections, onRefresh }) {
  const [name, setName] = useState('');
  const [fields, setFields] = useState([emptyField()]);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);

  function updateField(index, key, value) {
    setFields(prev => prev.map((f, i) => i === index ? { ...f, [key]: value } : f));
  }

  function addField() {
    setFields(prev => [...prev, emptyField()]);
  }

  function removeField(index) {
    setFields(prev => prev.filter((_, i) => i !== index));
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setError('');
    setSuccess('');

    const trimmedName = name.trim();
    if (!trimmedName) {
      setError('Collection name is required');
      return;
    }

    const validFields = fields.filter(f => f.name.trim());
    if (validFields.length === 0) {
      setError('At least one field is required');
      return;
    }

    setLoading(true);
    try {
      await createCollection(trimmedName, validFields.map(f => ({
        name: f.name.trim(),
        type: f.type,
        indexed: f.indexed,
      })));
      setSuccess(`Collection "${trimmedName}" created`);
      setName('');
      setFields([emptyField()]);
      onRefresh();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function handleDelete(collectionName) {
    if (!confirm(`Delete collection "${collectionName}" and all its data?`)) return;
    try {
      await deleteCollection(collectionName);
      onRefresh();
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div>
      <div className="form-section">
        <h2>Create Collection</h2>
        {error && <div className="msg error">{error}</div>}
        {success && <div className="msg success">{success}</div>}
        <form onSubmit={handleSubmit}>
          <div className="form-row" style={{ marginBottom: 16 }}>
            <input
              type="text"
              placeholder="Collection name"
              value={name}
              onChange={e => setName(e.target.value)}
            />
          </div>

          <div style={{ marginBottom: 8, fontWeight: 600, fontSize: '0.9rem' }}>Fields</div>
          {fields.map((field, i) => (
            <div className="form-row" key={i}>
              <input
                type="text"
                placeholder="Field name (e.g. address.city)"
                value={field.name}
                onChange={e => updateField(i, 'name', e.target.value)}
              />
              <select value={field.type} onChange={e => updateField(i, 'type', e.target.value)}>
                {FIELD_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
              <label>
                <input
                  type="checkbox"
                  checked={field.indexed}
                  onChange={e => updateField(i, 'indexed', e.target.checked)}
                />
                Indexed
              </label>
              {fields.length > 1 && (
                <button type="button" className="secondary" onClick={() => removeField(i)}>
                  Remove
                </button>
              )}
            </div>
          ))}

          <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
            <button type="button" className="secondary" onClick={addField}>Add Field</button>
            <button type="submit" className="primary" disabled={loading}>
              {loading ? 'Creating...' : 'Create Collection'}
            </button>
          </div>
        </form>
      </div>

      <div className="form-section">
        <h2>Existing Collections</h2>
        {collections.length === 0 ? (
          <p style={{ color: '#999', fontSize: '0.9rem' }}>No collections yet</p>
        ) : (
          <ul className="collection-list">
            {collections.map(c => (
              <li key={c.collectionName}>
                <div>
                  <span className="name">{c.collectionName}</span>
                  <div className="fields">
                    {c.fields.map(f =>
                      `${f.name} (${f.type}${f.indexed ? ', indexed' : ''})`
                    ).join(' | ')}
                  </div>
                </div>
                <button className="danger" onClick={() => handleDelete(c.collectionName)}>
                  Delete
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
