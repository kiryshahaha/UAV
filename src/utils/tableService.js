const API_BASE = '/api';

const getSessionId = () => {
  return localStorage.getItem('session_id');
};

const setSessionId = (sessionId) => {
  if (sessionId) {
    localStorage.setItem('session_id', sessionId);
    console.log("💾 Session ID сохранен в localStorage:", sessionId);
  }
};

// Сохраняем выбранную таблицу
const setCurrentTableName = (tableName) => {
  localStorage.setItem('current_table_name', tableName);
  console.log("💾 Текущая таблица сохранена:", tableName);
};

const getCurrentTableName = () => {
  return localStorage.getItem('current_table_name');
};

const createHeaders = (contentType = 'application/json') => {
  const headers = {
    'Content-Type': contentType,
  };
  
  const sessionId = getSessionId();
  if (sessionId) {
    headers['X-Session-ID'] = sessionId;
  }
  
  return headers;
};

export const tableService = {
  async getTables() {
    console.log("📡 GET /api/tables");
    try {
      const response = await fetch(`${API_BASE}/tables`, {
        method: 'GET',
        headers: createHeaders(),
      });
      
      if (!response.ok) {
        throw new Error(`Ошибка получения таблиц: ${response.status}`);
      }
      
      const data = await response.json();
      console.log("✅ Таблицы получены:", data);
      return data;
    } catch (error) {
      console.error("❌ Ошибка сети при получении таблиц:", error);
      throw error;
    }
  },

  async selectTable(tableName) {
    console.log("📡 POST /api/tables/select", tableName);
    try {
      const response = await fetch(`${API_BASE}/tables/select`, {
        method: 'POST',
        headers: createHeaders(),
        body: JSON.stringify({ 
          table_name: tableName
        }),
      });
      
      if (!response.ok) {
        throw new Error(`Ошибка выбора таблицы: ${response.status}`);
      }
      
      const data = await response.json();
      console.log("✅ Таблица выбрана:", data);
      
      // Сохраняем session_id и имя таблицы
      if (data.session_id) {
        setSessionId(data.session_id);
      }
      setCurrentTableName(tableName);
      
      return data;
    } catch (error) {
      console.error("❌ Ошибка сети при выборе таблицы:", error);
      throw error;
    }
  },

  async getCurrentTable() {
    console.log("📡 GET /api/tables/current");
    try {
      const response = await fetch(`${API_BASE}/tables/current`, {
        method: 'GET',
        headers: createHeaders(),
      });
      
      if (!response.ok) {
        throw new Error(`Ошибка получения текущей таблицы: ${response.status}`);
      }
      
      const data = await response.json();
      console.log("✅ Текущая таблица:", data);
      return data;
    } catch (error) {
      console.error("❌ Ошибка сети при получении текущей таблицы:", error);
      throw error;
    }
  },

  // Вспомогательные методы
  getStoredCurrentTable: getCurrentTableName, // Добавляем этот метод
  getStoredSessionId: getSessionId,
  clearSession: () => {
    localStorage.removeItem('session_id');
    localStorage.removeItem('current_table_name');
    console.log("🧹 Session ID и таблица очищены");
  },

  async deleteTable(tableName) {
  console.log("📡 DELETE /api/tables/", tableName);
  try {
    const response = await fetch(`${API_BASE}/tables/${tableName}`, {
      method: 'DELETE',
      headers: createHeaders(),
    });
    
    if (!response.ok) {
      throw new Error(`Ошибка удаления таблицы: ${response.status}`);
    }
    
    const data = await response.json();
    console.log("✅ Таблица удалена:", data);
    return data;
  } catch (error) {
    console.error("❌ Ошибка сети при удалении таблицы:", error);
    throw error;
  }
},

async uploadFile(file, uploadType = 'new') {
  console.log("📡 POST /api/upload", file.name, uploadType);
  try {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('uploadType', uploadType);

    const response = await fetch(`/api/upload`, {
      method: 'POST',
      body: formData,
      headers: {
        'X-Session-ID': getSessionId() || '',
      },
    });
    
    if (!response.ok) {
      throw new Error(`Ошибка загрузки файла: ${response.status}`);
    }
    
    const data = await response.json();
    console.log("✅ Файл загружен:", data);
    return data;
  } catch (error) {
    console.error("❌ Ошибка сети при загрузке файла:", error);
    throw error;
  }
},
};

