import { ConversationMessage } from './claude-client';
import crypto from 'crypto';

interface Conversation {
  id: string;
  messages: ConversationMessage[];
  createdAt: Date;
  lastActivityAt: Date;
  metadata?: {
    userId?: string;
    sessionType?: string;
  };
}

export class ConversationManager {
  private conversations: Map<string, Conversation>;
  private readonly maxConversationAge: number; // in milliseconds
  private readonly maxConversations: number;

  constructor(
    maxConversationAge: number = 1000 * 60 * 60 * 24, // 24 hours default
    maxConversations: number = 1000
  ) {
    this.conversations = new Map();
    this.maxConversationAge = maxConversationAge;
    this.maxConversations = maxConversations;
    
    // Start cleanup interval
    this.startCleanupInterval();
  }

  /**
   * Create a new conversation
   */
  createConversation(metadata?: { userId?: string; sessionType?: string }): string {
    const conversationId = this.generateConversationId();
    
    this.conversations.set(conversationId, {
      id: conversationId,
      messages: [],
      createdAt: new Date(),
      lastActivityAt: new Date(),
      metadata
    });

    // Clean up if we exceed max conversations
    if (this.conversations.size > this.maxConversations) {
      this.cleanupOldConversations();
    }

    return conversationId;
  }

  /**
   * Get a conversation by ID
   */
  getConversation(conversationId: string): Conversation | null {
    const conversation = this.conversations.get(conversationId);
    
    if (!conversation) {
      return null;
    }

    // Check if conversation is expired
    if (this.isConversationExpired(conversation)) {
      this.conversations.delete(conversationId);
      return null;
    }

    return conversation;
  }

  /**
   * Add a message to a conversation
   */
  addMessage(
    conversationId: string, 
    message: ConversationMessage
  ): boolean {
    const conversation = this.getConversation(conversationId);
    
    if (!conversation) {
      return false;
    }

    conversation.messages.push(message);
    conversation.lastActivityAt = new Date();
    
    return true;
  }

  /**
   * Get messages from a conversation
   */
  getMessages(conversationId: string): ConversationMessage[] {
    const conversation = this.getConversation(conversationId);
    return conversation ? conversation.messages : [];
  }

  /**
   * Update conversation activity timestamp
   */
  updateActivity(conversationId: string): void {
    const conversation = this.conversations.get(conversationId);
    if (conversation) {
      conversation.lastActivityAt = new Date();
    }
  }

  /**
   * Delete a conversation
   */
  deleteConversation(conversationId: string): boolean {
    return this.conversations.delete(conversationId);
  }

  /**
   * Get conversation statistics
   */
  getStats(): {
    totalConversations: number;
    activeConversations: number;
    oldestConversationAge: number | null;
  } {
    const now = Date.now();
    let oldestAge: number | null = null;
    let activeCount = 0;

    this.conversations.forEach((conversation) => {
      const age = now - conversation.createdAt.getTime();
      
      if (oldestAge === null || age > oldestAge) {
        oldestAge = age;
      }

      // Consider active if used in last hour
      if (now - conversation.lastActivityAt.getTime() < 1000 * 60 * 60) {
        activeCount++;
      }
    });

    return {
      totalConversations: this.conversations.size,
      activeConversations: activeCount,
      oldestConversationAge: oldestAge
    };
  }

  /**
   * Generate a unique conversation ID
   */
  private generateConversationId(): string {
    return `conv_${Date.now()}_${crypto.randomBytes(8).toString('hex')}`;
  }

  /**
   * Check if a conversation has expired
   */
  private isConversationExpired(conversation: Conversation): boolean {
    const age = Date.now() - conversation.lastActivityAt.getTime();
    return age > this.maxConversationAge;
  }

  /**
   * Clean up old conversations
   */
  private cleanupOldConversations(): void {
    const conversationsArray = Array.from(this.conversations.entries());
    
    // Sort by last activity (oldest first)
    conversationsArray.sort((a, b) => 
      a[1].lastActivityAt.getTime() - b[1].lastActivityAt.getTime()
    );

    // Remove expired conversations
    for (const [id, conversation] of conversationsArray) {
      if (this.isConversationExpired(conversation)) {
        this.conversations.delete(id);
      }
    }

    // If still over limit, remove oldest
    while (this.conversations.size > this.maxConversations) {
      const oldestId = conversationsArray[0][0];
      this.conversations.delete(oldestId);
      conversationsArray.shift();
    }
  }

  /**
   * Start periodic cleanup
   */
  private startCleanupInterval(): void {
    // Run cleanup every hour
    setInterval(() => {
      this.cleanupOldConversations();
    }, 1000 * 60 * 60);
  }
}

// Singleton instance
let conversationManager: ConversationManager | null = null;

export function getConversationManager(): ConversationManager {
  if (!conversationManager) {
    conversationManager = new ConversationManager();
  }
  return conversationManager;
}