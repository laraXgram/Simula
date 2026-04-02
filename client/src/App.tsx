import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import TelegramChatPage from './components/Chat/TelegramChatPage';

function App() {
  return (
    <BrowserRouter>
      <div className="h-screen text-telegram-text">
        <Routes>
          <Route path="/" element={<Navigate to="/chat" replace />} />
          <Route path="/chat" element={<TelegramChatPage initialTab="chats" />} />
          <Route path="/bots" element={<TelegramChatPage initialTab="bots" />} />
          <Route path="/debug" element={<SimplePage title="Debug Panel will be added in phase 1.10" />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

function SimplePage({ title }: { title: string }) {
  return (
    <div className="flex h-full items-center justify-center bg-app-pattern">
      <div className="text-center">
        <h1 className="mb-2 text-3xl font-semibold">LaraGram Studio</h1>
        <p className="text-telegram-textSecondary">{title}</p>
      </div>
    </div>
  );
}

export default App;
