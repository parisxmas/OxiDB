import React, { useState, useEffect, useCallback } from 'react';
import { listCollections } from './api.js';
import CollectionManager from './CollectionManager.jsx';
import DocumentUpload from './DocumentUpload.jsx';
import SearchScreen from './SearchScreen.jsx';

const TABS = ['Collections', 'Upload', 'Search'];

export default function App() {
  const [tab, setTab] = useState('Collections');
  const [collections, setCollections] = useState([]);
  const [selectedCollection, setSelectedCollection] = useState('');

  const refreshCollections = useCallback(async () => {
    try {
      const list = await listCollections();
      setCollections(list);
      setSelectedCollection(prev => {
        if (list.length > 0 && !list.find(c => c.collectionName === prev)) {
          return list[0].collectionName;
        }
        return prev;
      });
    } catch (e) {
      console.error('Failed to load collections:', e);
    }
  }, []);

  useEffect(() => {
    refreshCollections();
  }, [refreshCollections]);

  return (
    <div className="app">
      <h1>OxiDB Document Manager</h1>
      <div className="tabs">
        {TABS.map(t => (
          <button
            key={t}
            className={tab === t ? 'active' : ''}
            onClick={() => setTab(t)}
          >
            {t}
          </button>
        ))}
      </div>

      {tab === 'Collections' && (
        <CollectionManager
          collections={collections}
          onRefresh={refreshCollections}
        />
      )}

      {tab === 'Upload' && (
        <DocumentUpload
          collections={collections}
          selectedCollection={selectedCollection}
          onSelectCollection={setSelectedCollection}
        />
      )}

      {tab === 'Search' && (
        <SearchScreen
          collections={collections}
          selectedCollection={selectedCollection}
          onSelectCollection={setSelectedCollection}
        />
      )}
    </div>
  );
}
