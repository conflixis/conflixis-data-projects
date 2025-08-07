'use client';

import { useState, useEffect, useRef } from 'react';
import { Send, AlertCircle, Loader2 } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import type { ChatMessage, SuggestedQuery, QueryResponse } from '@/types';

export default function Home() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [suggestedQueries, setSuggestedQueries] = useState<SuggestedQuery[]>([]);
  const [conversationId, setConversationId] = useState<string | undefined>();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Load suggested queries
    fetch('/api/suggested-queries')
      .then(res => res.json())
      .then(data => setSuggestedQueries(data))
      .catch(console.error);
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: input,
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    try {
      const response = await fetch('/api/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          query: input,
          conversationId: conversationId,
          enableConversation: true
        }),
      });

      const data: QueryResponse = await response.json();

      // Update conversation ID if returned
      if (data.conversationId) {
        setConversationId(data.conversationId);
      }

      if (data.success) {
        // Add reasoning if available
        if (data.sqlGenerated) {
          setMessages(prev => [...prev, {
            id: Date.now().toString() + '-sql',
            role: 'assistant',
            content: '```sql\n' + data.sqlGenerated + '\n```',
            type: 'sql',
            timestamp: new Date(),
          } as ChatMessage]);
        }

        // Add cost estimate
        if (data.queryCost !== undefined) {
          setMessages(prev => [...prev, {
            id: Date.now().toString() + '-cost',
            role: 'system',
            content: `Estimated query cost: $${data.queryCost?.toFixed(4) || '0.0000'}`,
            type: 'cost',
            timestamp: new Date(),
          } as ChatMessage]);
        }

        // Add main analysis
        if (data.complianceAnalysis) {
          setMessages(prev => [...prev, {
            id: Date.now().toString() + '-analysis',
            role: 'assistant',
            content: data.complianceAnalysis,
            timestamp: new Date(),
          } as ChatMessage]);
        }
      } else {
        setMessages(prev => [...prev, {
          id: Date.now().toString() + '-error',
          role: 'system',
          content: data.error || 'An error occurred',
          type: 'error',
          timestamp: new Date(),
        } as ChatMessage]);
      }
    } catch (error) {
      setMessages(prev => [...prev, {
        id: Date.now().toString() + '-error',
        role: 'system',
        content: 'Failed to connect to the server',
        type: 'error',
        timestamp: new Date(),
      } as ChatMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSuggestedQuery = (query: string) => {
    setInput(query);
  };

  return (
    <div className="flex h-screen bg-conflixis-ivory">
      {/* Main Content */}
      <div className="flex-1 flex flex-col">
        {/* Header */}
        <header className="bg-conflixis-white shadow-sm border-b border-conflixis-tan/20">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex items-center justify-between h-16">
              <div className="flex items-center space-x-4">
                <img 
                  src="/conflixis-logo.png" 
                  alt="Conflixis" 
                  className="h-10 w-auto"
                />
                <div>
                  <h1 className="text-2xl font-soehneKraftig text-conflixis-green">
                    Healthcare Compliance Analysis Bot
                  </h1>
                  <p className="text-sm text-conflixis-green/70">
                    AI-powered fraud, waste, and abuse detection
                  </p>
                </div>
              </div>
              <div className="flex items-center space-x-4">
                {conversationId && (
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-conflixis-green/70">Conversation:</span>
                    <span className="text-xs font-mono text-conflixis-green/50">
                      {conversationId.substring(0, 12)}...
                    </span>
                    <button
                      onClick={() => {
                        setConversationId(undefined);
                        setMessages([]);
                      }}
                      className="text-xs text-conflixis-blue hover:text-conflixis-blue/80 transition-colors"
                    >
                      New conversation
                    </button>
                  </div>
                )}
                <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-soehneLight bg-conflixis-mint/20 text-conflixis-green">
                  <span className="w-2 h-2 bg-conflixis-mint rounded-full mr-2"></span>
                  Connected
                </span>
              </div>
            </div>
          </div>
        </header>

        {/* Chat Container */}
        <div className="flex-1 overflow-hidden flex">
          <div className="flex-1 flex flex-col">
            {/* Messages */}
            <div className="flex-1 overflow-y-auto p-4 space-y-4 custom-scrollbar">
              {messages.length === 0 && (
                <div className="text-center text-conflixis-green/60 py-8">
                  <p className="text-lg font-soehneKraftig mb-2">
                    Welcome to the Compliance Analysis Bot
                  </p>
                  <p className="text-sm">
                    Ask questions about physician payments to uncover potential compliance risks
                  </p>
                </div>
              )}
              
              {messages.map((message) => (
                <div
                  key={message.id}
                  className={`flex ${
                    message.role === 'user' ? 'justify-end' : 'justify-start'
                  }`}
                >
                  <div
                    className={`max-w-2xl rounded-lg px-4 py-2 shadow-sm ${
                      message.role === 'user'
                        ? 'bg-conflixis-green text-conflixis-white'
                        : message.type === 'error'
                        ? 'bg-conflixis-red/10 text-conflixis-red border border-conflixis-red/20'
                        : message.type === 'cost'
                        ? 'bg-conflixis-blue/10 text-conflixis-blue border border-conflixis-blue/20'
                        : message.type === 'sql'
                        ? 'bg-conflixis-green text-conflixis-white'
                        : 'bg-conflixis-white text-conflixis-green border border-conflixis-tan/20'
                    }`}
                  >
                    {message.type === 'sql' ? (
                      <pre className="overflow-x-auto">
                        <code className="text-sm">
                          {message.content.replace(/```sql\n|```/g, '')}
                        </code>
                      </pre>
                    ) : (
                      <ReactMarkdown 
                        className={`prose prose-sm max-w-none ${
                          message.role === 'user' 
                            ? 'prose-invert' 
                            : 'prose-conflixis'
                        }`}
                      >
                        {message.content}
                      </ReactMarkdown>
                    )}
                  </div>
                </div>
              ))}
              
              {isLoading && (
                <div className="flex justify-start">
                  <div className="bg-conflixis-white rounded-lg px-4 py-2 border border-conflixis-tan/20 shadow-sm">
                    <div className="flex items-center space-x-2">
                      <Loader2 className="w-4 h-4 animate-spin text-conflixis-mint" />
                      <span className="text-conflixis-green">Analyzing...</span>
                    </div>
                  </div>
                </div>
              )}
              
              <div ref={messagesEndRef} />
            </div>

            {/* Input */}
            <form onSubmit={handleSubmit} className="p-4 border-t border-conflixis-tan/20 bg-conflixis-white">
              <div className="flex space-x-2">
                <input
                  type="text"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder="Ask about payment patterns, e.g., 'Show me high-risk speaker fee patterns in Texas'"
                  className="flex-1 px-4 py-2 border border-conflixis-tan/30 rounded-lg bg-conflixis-white text-conflixis-green placeholder-conflixis-green/40 focus:outline-none focus:ring-2 focus:ring-conflixis-mint focus:border-conflixis-mint"
                  disabled={isLoading}
                />
                <button
                  type="submit"
                  disabled={isLoading || !input.trim()}
                  className="px-6 py-2 bg-conflixis-green text-conflixis-white rounded-lg hover:bg-conflixis-green/90 focus:outline-none focus:ring-2 focus:ring-conflixis-mint disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  {isLoading ? (
                    <Loader2 className="w-5 h-5 animate-spin" />
                  ) : (
                    <Send className="w-5 h-5" />
                  )}
                </button>
              </div>
            </form>
          </div>

          {/* Sidebar */}
          <div className="w-80 border-l border-conflixis-tan/20 bg-conflixis-white p-4 overflow-y-auto">
            <h3 className="font-soehneKraftig text-conflixis-green mb-3">
              Suggested Analyses
            </h3>
            <div className="space-y-2">
              {suggestedQueries.map((query, index) => (
                <button
                  key={index}
                  onClick={() => handleSuggestedQuery(query.query)}
                  className="w-full text-left p-3 rounded-lg bg-conflixis-ivory hover:bg-conflixis-tan/10 border border-conflixis-tan/20 transition-colors"
                >
                  <div className="font-medium text-sm text-conflixis-green">
                    {query.title}
                  </div>
                  <div className="text-xs text-conflixis-green/70 mt-1">
                    {query.description}
                  </div>
                </button>
              ))}
            </div>

            <div className="mt-6">
              <h3 className="font-soehneKraftig text-conflixis-green mb-3">
                Compliance Focus Areas
              </h3>
              <ul className="space-y-2 text-sm text-conflixis-green/80">
                <li className="flex items-start">
                  <AlertCircle className="w-4 h-4 text-conflixis-gold mr-2 mt-0.5" />
                  <span>High-frequency, low-value payments</span>
                </li>
                <li className="flex items-start">
                  <AlertCircle className="w-4 h-4 text-conflixis-gold mr-2 mt-0.5" />
                  <span>Ownership interests with high payments</span>
                </li>
                <li className="flex items-start">
                  <AlertCircle className="w-4 h-4 text-conflixis-gold mr-2 mt-0.5" />
                  <span>Payments from competing manufacturers</span>
                </li>
                <li className="flex items-start">
                  <AlertCircle className="w-4 h-4 text-conflixis-gold mr-2 mt-0.5" />
                  <span>Sudden year-over-year increases</span>
                </li>
                <li className="flex items-start">
                  <AlertCircle className="w-4 h-4 text-conflixis-gold mr-2 mt-0.5" />
                  <span>Luxury travel and entertainment</span>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}