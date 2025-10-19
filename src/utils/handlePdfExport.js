/**
 * Утилита для генерации PDF отчетов через бэкенд FastAPI
 */

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';

/**
 * Генерирует PDF отчет с заданными параметрами
 * @param {Object} params - Параметры отчета
 * @param {string} params.report_type - Тип отчета
 * @param {string} params.date_from - Начальная дата (YYYY-MM-DD)
 * @param {string} params.date_to - Конечная дата (YYYY-MM-DD)
 * @param {Array} params.regions - Список регионов
 * @param {Array} params.operator_names - Список операторов (опционально)
 * @returns {Promise<Object>} Результат операции
 */
export const handlePdfExport = async (params) => {
  try {
    const { report_type, date_from, date_to, regions = [], operator_names = [] } = params;

    // Валидация параметров
    if (!report_type) {
      throw new Error('Тип отчета обязателен');
    }

    // Формируем запрос для бэкенда FastAPI
    const exportRequest = {
      report_type,
      date_from,
      date_to,
      regions,
      operator_names
    };

    console.log('📊 Отправка запроса на генерацию PDF:', exportRequest);

    // Вызываем бэкенд FastAPI
    const response = await fetch(`${BACKEND_URL}/export/report`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(exportRequest),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('❌ Ошибка от бэкенда:', response.status, errorText);
      throw new Error(`Ошибка сервера: ${response.status} - ${errorText}`);
    }

    // Получаем PDF файл
    const pdfBlob = await response.blob();
    
    // Проверяем, что это действительно PDF
    if (!pdfBlob.type.includes('pdf')) {
      throw new Error('Получен некорректный файл');
    }

    // Создаем URL для скачивания
    const pdfUrl = URL.createObjectURL(pdfBlob);
    
    // Генерируем имя файла
    const timestamp = new Date().toISOString().slice(0, 10);
    const regionSuffix = regions.length > 0 ? `_${regions[0]}` : '';
    const fileName = `uav_report_${report_type}${regionSuffix}_${timestamp}.pdf`;

    return {
      success: true,
      pdfUrl,
      fileName,
      blob: pdfBlob,
      fileSize: pdfBlob.size
    };

  } catch (error) {
    console.error('❌ Ошибка при генерации PDF:', error);
    
    return {
      success: false,
      message: error.message || 'Неизвестная ошибка при генерации PDF',
      error: error
    };
  }
};

/**
 * Скачивает PDF файл
 * @param {string} pdfUrl - URL объекта Blob
 * @param {string} fileName - Имя файла для скачивания
 */
export const downloadPdf = (pdfUrl, fileName) => {
  try {
    const link = document.createElement('a');
    link.href = pdfUrl;
    link.download = fileName;
    link.style.display = 'none';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Освобождаем память
    setTimeout(() => URL.revokeObjectURL(pdfUrl), 100);
    
    return true;
  } catch (error) {
    console.error('❌ Ошибка при скачивании PDF:', error);
    return false;
  }
};

/**
 * Утилита для массовой генерации отчетов
 * @param {Array} reports - Массив параметров отчетов
 * @returns {Promise<Array>} Результаты генерации
 */
export const generateMultiplePdfReports = async (reports) => {
  const results = [];
  
  for (const reportParams of reports) {
    try {
      const result = await handlePdfExport(reportParams);
      results.push(result);
      
      // Небольшая задержка между запросами
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      results.push({
        success: false,
        message: error.message,
        params: reportParams
      });
    }
  }
  
  return results;
};

/**
 * Получает доступные типы отчетов
 * @returns {Array} Список типов отчетов
 */
export const getAvailableReportTypes = () => {
  return [
    {
      value: "general",
      label: "Общий отчет",
      description: "Общая статистика по всем показателям"
    },
    {
      value: "regions",
      label: "Отчет по регионам", 
      description: "Детальная статистика по регионам"
    },
    {
      value: "operators",
      label: "Отчет по операторам",
      description: "Топ операторы и их активность"
    },
    {
      value: "operator_detail",
      label: "Детальный отчет по оператору",
      description: "Подробная информация по конкретному оператору"
    }
  ];
};

export default handlePdfExport;