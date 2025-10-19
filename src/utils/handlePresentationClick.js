import PptxGenJS from "pptxgenjs";

/**
 * Размеры слайда LAYOUT_WIDE (16:9) в дюймах
 */
const SLIDE_W = 13.333;
const SLIDE_H = 7.5;

/**
 * Рассчитывает координату X для горизонтального центрирования 
 * @param {number} objW Ширина 
 * @returns {number} Координата X
 */
const getCenterX = (objW) => (SLIDE_W - objW) / 2;

/**
 * Форматирует текст безопасно
 */
function safeText(text) {
  if (text === null || text === undefined) return "Н/Д";
  return String(text);
}

/**
 * Создает удобочитаемую метку для файла
 */
function makeFileName({ date_from, date_to, region }) {
  const datePart = `${date_from || "all"}_${date_to || "all"}`.replace(
    /[:\/\s]/g,
    "-"
  );
  const regionPart = (region || "all").replace(/\s+/g, "-");
  return `bpla_report_${datePart}_${regionPart}_${Date.now()}.pptx`;
}

export const handlePresentationClick = async ({
  date_from,
  date_to,
  region,
  limit,
} = {}) => {
  const API_URL = process.env.NEXT_PUBLIC_API_URL || "";

  // Инициализация презентации
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";

  const styles = {
    title: {
      fontSize: 36,
      color: "#1A365D",
      bold: true,
      align: "center",
      // valign: "middle",
    },
    subtitle: {
      fontSize: 18,
      color: "#4A5568",
      align: "center",
      // valign: "middle",
    },
    heading: {
      fontSize: 26,
      color: "#1A365D",
      bold: true,
      align: "center",
      // valign: "middle",
    },
    body: { fontSize: 14, color: "#2D3748", align: "center", valign: "middle" },
    highlight: {
      fontSize: 16,
      color: "#1A365D",
      bold: true,
      align: "center",
      // valign: "middle",
    },
    note: {
      fontSize: 11,
      color: "#718096",
      italic: true,
      align: "center",
      // valign: "middle",
    },
  };

  const bgColor = "#F0F8FF";

  try {
    console.log("Начало генерации презентации...");

    const [
      dashboardDataResult,
      regionsDataResult,
      operatorsDataResult,
      monthlyDataResult,
    ] = await Promise.all([
      fetchDashboardData({ API_URL, date_from, date_to, region, limit }).catch(
        (e) => {
          console.warn("fetchDashboardData failed:", e);
          return null;
        }
      ),
      fetchRegionsData({ API_URL, date_from, date_to, limit }).catch((e) => {
        console.warn("fetchRegionsData failed:", e);
        return { regions: [] };
      }),
      fetchOperatorsData({ API_URL, date_from, date_to, region }).catch((e) => {
        console.warn("fetchOperatorsData failed:", e);
        return { operators: [] };
      }),
      fetchMonthlyData({ API_URL, date_from, date_to }).catch((e) => {
        console.warn("fetchMonthlyData failed:", e);
        return { regions: [] };
      }),
    ]);

    const dashboardData = dashboardDataResult || {};
    const regionsData = regionsDataResult || { regions: [] };
    const operatorsData = operatorsDataResult || { operators: [] };
    const monthlyData = monthlyDataResult || { regions: [] };

    addTitleSlide(pptx, { date_from, date_to, region, styles, bgColor });
    addKpiSlide(pptx, { dashboardData, styles, bgColor });
    addRegionsSlide(pptx, {
      pptx,
      regionsData,
      dashboardData,
      styles,
      bgColor,
    });
    addOperatorsSlide(pptx, { operatorsData, dashboardData, styles, bgColor });
    addAircraftTypesSlide(pptx, { dashboardData, styles, bgColor });
    addTimeAnalyticsSlide(pptx, {
      pptx,
      dashboardData,
      monthlyData,
      styles,
      bgColor,
    });
    addTechParamsSlide(pptx, { dashboardData, styles, bgColor });
    addConclusionSlide(pptx, { dashboardData, styles, bgColor });

    // Сохранение
    const fileName = makeFileName({ date_from, date_to, region });
    await pptx.writeFile({ fileName });

    console.log("Презентация создана:", fileName);

    return {
      success: true,
      fileName,
      slidesCount: pptx.slides.length,
      dataSources: {
        dashboard: API_URL ? `${API_URL}/dashboard/stats` : "/dashboard/stats",
        regions: API_URL ? `${API_URL}/regions/stats` : "/regions/stats",
        operators: API_URL
          ? `${API_URL}/operators/top/all`
          : "/operators/top/all",
        monthly: API_URL
          ? `${API_URL}/stats/regions/monthly`
          : "/stats/regions/monthly",
      },
      statistics: {
        totalFlights: dashboardData.general_stats?.total_flights || 0,
        totalRegions: dashboardData.general_stats?.total_regions || 0,
        totalOperators: dashboardData.general_stats?.total_operators || 0,
        totalAircrafts: dashboardData.general_stats?.total_aircrafts || 0,
      },
      message: `Презентация успешно создана — ${pptx.slides.length} слайдов.`,
    };
  } catch (error) {
    console.error("❌ Ошибка при создании презентации:", error);
    return {
      success: false,
      message: `Ошибка при создании презентации: ${safeText(
        error?.message || error
      )}`,
    };
  }
};

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(
      `HTTP ${res.status} ${res.statusText} ${text ? "- " + text : ""}`
    );
  }
  return res.json();
}

async function fetchDashboardData({
  API_URL,
  date_from,
  date_to,
  region,
  limit,
}) {
  const url = new URL(
    `${API_URL}/dashboard/stats`,
    API_URL ? undefined : window.location.origin
  );
  if (date_from) url.searchParams.append("date_from", date_from);
  if (date_to) url.searchParams.append("date_to", date_to);
  if (region) url.searchParams.append("region", region);
  if (limit) url.searchParams.append("limit", limit);
  return fetchJSON(url.toString());
}

async function fetchRegionsData({ API_URL, date_from, date_to, limit }) {
  const url = new URL(
    `${API_URL}/regions/stats`,
    API_URL ? undefined : window.location.origin
  );
  if (date_from) url.searchParams.append("date_from", date_from);
  if (date_to) url.searchParams.append("date_to", date_to);
  url.searchParams.append("limit", limit || 10);
  return fetchJSON(url.toString());
}

async function fetchOperatorsData({ API_URL, date_from, date_to, region }) {
  const url = new URL(
    `${API_URL}/operators/top/all`,
    API_URL ? undefined : window.location.origin
  );
  if (date_from) url.searchParams.append("date_from", date_from);
  if (date_to) url.searchParams.append("date_to", date_to);
  if (region) url.searchParams.append("region", region);
  url.searchParams.append("limit", 15);
  return fetchJSON(url.toString());
}

async function fetchMonthlyData({ API_URL, date_from, date_to }) {
  const url = new URL(
    `${API_URL}/stats/regions/monthly`,
    API_URL ? undefined : window.location.origin
  );
  if (date_from) url.searchParams.append("date_from", date_from);
  if (date_to) url.searchParams.append("date_to", date_to);
  return fetchJSON(url.toString());
}

// ====================================================================
// Слайды с примененным центрированием
// ====================================================================

function addTitleSlide(pptx, { date_from, date_to, region, styles, bgColor }) {
  const slide = pptx.addSlide();
  slide.background = { color: bgColor };

  const TITLE_W = 9;
  const SUBTITLE_W = 9;
  const NOTE_W = 9;

  // Главный заголовок: x рассчитан для w=9
  slide.addText("ОТЧЕТ ПО ПОЛЕТАМ БПЛА", {
    x: getCenterX(TITLE_W),  
    y: 2,
    w: TITLE_W,
    h: 1,
    ...styles.title,
  });

  let periodInfo = "За весь период";
  if (date_from && date_to) periodInfo = `С ${date_from} по ${date_to}`;
  else if (date_from) periodInfo = `С ${date_from}`;
  else if (date_to) periodInfo = `По ${date_to}`;

  const regionInfo = region ? `Регион: ${region}` : "Все регионы";
  // Подзаголовок: x рассчитан для w=9
  slide.addText(`${periodInfo}\n${regionInfo}`, {
    x: getCenterX(SUBTITLE_W),  
    y: 3.2,
    w: SUBTITLE_W,
    h: 0.8,
    ...styles.subtitle,
  });

  // Примечание: x рассчитан для w=9
  slide.addText(`Сгенерировано: ${new Date().toLocaleDateString("ru-RU")}`, {
    x: getCenterX(NOTE_W),  
    y: 4.5,
    w: NOTE_W,
    h: 0.4,
    ...styles.note,
  });
}

function addKpiSlide(pptx, { dashboardData, styles, bgColor }) {
  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const KPI_TABLE_W = 7;
  const PARAMS_HEADING_W = 9;
  const PARAMS_TABLE_W = 7;

  // Заголовок слайда
  slide.addText("Ключевые показатели", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  const generalStats = dashboardData.general_stats || {};
  const kpiData = [
    ["Показатель", "Значение"],
    [
      "Всего полетов",
      (generalStats.total_flights || 0).toLocaleString("ru-RU") ||
        String(generalStats.total_flights || 0),
    ],
    [
      "Количество регионов",
      (generalStats.total_regions || 0).toLocaleString("ru-RU") ||
        String(generalStats.total_regions || 0),
    ],
    [
      "Уникальных БПЛА",
      (generalStats.total_aircrafts || 0).toLocaleString("ru-RU") ||
        String(generalStats.total_aircrafts || 0),
    ],
    [
      "Операторов",
      (generalStats.total_operators || 0).toLocaleString("ru-RU") ||
        String(generalStats.total_operators || 0),
    ],
  ];

  // Таблица KPI
  slide.addTable(kpiData, {
    x: getCenterX(KPI_TABLE_W),  
    y: 1.8,
    w: KPI_TABLE_W,
    colW: [4, 3],
    border: { pt: 1, color: "#CBD5E0" },
    fill: { color: "#FFFFFF" },
    color: "#2D3748",
    fontSize: 14,
    align: "center",
    valign: "middle",
  });

  let yPos = 3.5;
  if (
    dashboardData.level_stats ||
    dashboardData.radius_stats ||
    dashboardData.duration_stats
  ) {
    // Подзаголовок
    slide.addText("Средние параметры полетов", {
      x: getCenterX(PARAMS_HEADING_W), 
      y: yPos,
      w: PARAMS_HEADING_W,
      h: 0.4,
      ...styles.highlight,
    });
    yPos += 0.6;
    const paramsData = [["Параметр", "Значение"]];
    if (dashboardData.level_stats?.avg_level) {
      paramsData.push([
        "Средняя высота",
        `${Math.round(dashboardData.level_stats.avg_level)} м`,
      ]);
    }
    if (dashboardData.radius_stats?.avg_radius) {
      paramsData.push([
        "Средний радиус",
        `${Math.round(dashboardData.radius_stats.avg_radius)} м`,
      ]);
    }
    if (dashboardData.duration_stats?.avg_duration_minutes) {
      const hours = Math.floor(
        dashboardData.duration_stats.avg_duration_minutes / 60
      );
      const minutes = Math.round(
        dashboardData.duration_stats.avg_duration_minutes % 60
      );
      paramsData.push(["Средняя продолжительность", `${hours}ч ${minutes}м`]);
    }
    // Таблица параметров
    slide.addTable(paramsData, {
      x: getCenterX(PARAMS_TABLE_W),  
      y: yPos,
      w: PARAMS_TABLE_W,
      colW: [4, 3],
      border: { pt: 1, color: "#CBD5E0" },
      fill: { color: "#FFFFFF" },
      color: "#2D3748",
      fontSize: 12,
      align: "center",
    });
  }
}

function addRegionsSlide(
  pptx,
  { regionsData, dashboardData, styles, bgColor }
) {
  const regions = (regionsData && regionsData.regions) || [];
  if (!regions.length) return;

  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const TABLE_W = 8.5;
  const FOOTER_W = 9;

  // Заголовок слайда
  slide.addText("Топ регионов по количеству полетов", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  const topRegions = regions.slice(0, 8);
  const regionsTableData = [
    ["Регион", "Полетов", "Операторов", "БПЛА", "Ср. высота"],
  ];
  topRegions.forEach((r) => {
    regionsTableData.push([
      safeText(r.region),
      (r.flight_count || 0).toLocaleString("ru-RU") ||
        String(r.flight_count || 0),
      (r.unique_operators || 0).toLocaleString("ru-RU") ||
        String(r.unique_operators || 0),
      (r.unique_aircrafts || 0).toLocaleString("ru-RU") ||
        String(r.unique_aircrafts || 0),
      r.statistics?.flight_level?.avg_level_m
        ? `${Math.round(r.statistics.flight_level.avg_level_m)} м`
        : "—",
    ]);
  });

  // Таблица регионов
  slide.addTable(regionsTableData, {
    x: getCenterX(TABLE_W),  
    y: 1.8,
    w: TABLE_W,
    colW: [3.5, 1.2, 1.2, 1.2, 1.4],
    border: { pt: 1, color: "#CBD5E0" },
    fill: { color: "#FFFFFF" },
    color: "#2D3748",
    fontSize: 11,
    align: "center",
  });

  try {
    const labels = topRegions.map((r) => safeText(r.region).slice(0, 20));
    const values = topRegions.map((r) => r.flight_count || 0);
    if (values.some((v) => v > 0)) {
      const chartSlide = pptx.addSlide();
      chartSlide.background = { color: bgColor };
      const CHART_HEADING_W = 9;
      const CHART_W = 9;

      chartSlide.addText("График: Полетов по топ регионам", {
        x: getCenterX(CHART_HEADING_W), 
        y: 0.8,
        w: CHART_HEADING_W,
        h: 0.5,
        ...styles.heading,
      });

      const dataSeries = [{ name: "Полетов", labels, values }];
      // График
      chartSlide.addChart(pptx.ChartType.bar, dataSeries, {
        x: getCenterX(CHART_W),  
        y: 1.6,
        w: CHART_W,
        h: 4,
        showLegend: false,
        showTitle: false,
        barGrouping: "clustered",
      });
    }
  } catch (e) {
    console.warn("Не удалось добавить график по регионам:", e);
  }

  if (dashboardData.active_region?.region) {
    // Примечание
    slide.addText(
      `Самый активный регион: ${dashboardData.active_region.region} (${
        dashboardData.active_region.flight_count || 0
      } полетов)`,
      {
        x: getCenterX(FOOTER_W),  
        y: 5.2,
        w: FOOTER_W,
        h: 0.4,
        ...styles.highlight,
      }
    );
  }
}

function addOperatorsSlide(
  pptx,
  { operatorsData, dashboardData, styles, bgColor }
) {
  const operators = (operatorsData && operatorsData.operators) || [];
  if (!operators.length) return;

  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const TABLE_W = 8.5;
  const FOOTER_W = 9;

  // Заголовок слайда
  slide.addText("Топ операторов", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  const topOperators = operators.slice(0, 10);
  const operatorsTableData = [
    ["Оператор", "Полетов", "БПЛА", "Регионов", "Ср. высота"],
  ];
  topOperators.forEach((op) => {
    operatorsTableData.push([
      op.name && op.name.length > 30
        ? op.name.substring(0, 30) + "..."
        : safeText(op.name),
      (op.flight_count || 0).toLocaleString("ru-RU") ||
        String(op.flight_count || 0),
      (op.unique_aircrafts || 0).toLocaleString("ru-RU") ||
        String(op.unique_aircrafts || 0),
      (op.regions_covered || 0).toLocaleString("ru-RU") ||
        String(op.regions_covered || 0),
      op.avg_level_m ? `${Math.round(op.avg_level_m)} м` : "—",
    ]);
  });

  // Таблица операторов
  slide.addTable(operatorsTableData, {
    x: getCenterX(TABLE_W),  
    y: 1.8,
    w: TABLE_W,
    colW: [3.5, 1.2, 1.2, 1.2, 1.4],
    border: { pt: 1, color: "#CBD5E0" },
    fill: { color: "#FFFFFF" },
    color: "#2D3748",
    fontSize: 10.5,
    align: "center",
  });

  if (dashboardData.operators && Array.isArray(dashboardData.operators)) {
    const operatorTypes = {};
    dashboardData.operators.forEach((op) => {
      const type = op.type || "Другое";
      operatorTypes[type] = (operatorTypes[type] || 0) + 1;
    });

    const typeText =
      "Распределение по типам: " +
      Object.entries(operatorTypes)
        .map(([type, count]) => `${type}: ${count}`)
        .join(", ");
    // Примечание
    slide.addText(typeText, {
      x: getCenterX(FOOTER_W),  
      y: 5.2,
      w: FOOTER_W,
      h: 0.4,
      ...styles.note,
    });
  }
}

function addAircraftTypesSlide(pptx, { dashboardData, styles, bgColor }) {
  const types = dashboardData.aircraft_types || [];
  if (!types.length) return;

  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const TABLE_W = 8.5;
  const FOOTER_W = 9;

  // Заголовок слайда
  slide.addText("Типы БПЛА", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  const topTypes = types.slice(0, 12);
  const tableData = [["#", "Тип БПЛА", "Количество полетов"]];
  topTypes.forEach((type, i) => {
    tableData.push([
      (i + 1).toString(),
      safeText(type.type || type.name),
      (type.count || 0).toLocaleString("ru-RU"),
    ]);
  });

  // Таблица типов БПЛА
  slide.addTable(tableData, {
    x: getCenterX(TABLE_W),  
    y: 1.8,
    w: TABLE_W,
    colW: [0.6, 5.5, 2.4],
    border: { pt: 1, color: "#CBD5E0" },
    fill: { color: "#FFFFFF" },
    fontSize: 12,
    color: "#2D3748",
    align: "center",
    valign: "middle",
    bold: true,
    autoPage: true,
  });

  try {
    const labels = topTypes.map((t) => safeText(t.type || t.name).slice(0, 25));
    const values = topTypes.map((t) => t.count || 0);
    if (values.some((v) => v > 0)) {
      const chartSlide = pptx.addSlide();
      chartSlide.background = { color: bgColor };
      const CHART_HEADING_W = 9;
      const CHART_W = 8.6;

      chartSlide.addText("Распределение типов БПЛА по числу полетов", {
        x: getCenterX(CHART_HEADING_W), 
        y: 0.8,
        w: CHART_HEADING_W,
        h: 0.6,
        ...styles.heading,
      });

      // График
      chartSlide.addChart(
        pptx.ChartType.bar,
        [{ name: "Полетов", labels, values }],
        {
          x: getCenterX(CHART_W),  
          y: 1.6,
          w: CHART_W,
          h: 4.2,
          showLegend: false,
          barGrouping: "clustered",
          dataLabelFormatCode: "0",
        }
      );
    }
  } catch (e) {
    console.warn("Не удалось добавить график по типам БПЛА:", e);
  }

  // Примечание
  slide.addText(`Всего различных типов БПЛА: ${types.length}`, {
    x: getCenterX(FOOTER_W),  
    y: 5.5,
    w: FOOTER_W,
    h: 0.4,
    ...styles.highlight,
  });
}

function addTimeAnalyticsSlide(
  pptx,
  { dashboardData, monthlyData, styles, bgColor }
) {
  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const SUBHEADING_W = 9;
  const BODY_W = 9;

  // Заголовок слайда
  slide.addText("Аналитика по времени", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  let yPos = 1.8;
  if (
    dashboardData.yearly_stats &&
    Object.keys(dashboardData.yearly_stats).length > 0
  ) {
    // Подзаголовок
    slide.addText("Статистика по годам:", {
      x: getCenterX(SUBHEADING_W), 
      y: yPos,
      w: SUBHEADING_W,
      h: 0.4,
      ...styles.highlight,
    });
    yPos += 0.6;

    const years = Object.keys(dashboardData.yearly_stats).sort((a, b) => b - a);
    years.slice(0, 5).forEach((year) => {
      const stats = dashboardData.yearly_stats[year];
      // Элементы списка
      slide.addText(
        `${year}: ${stats.flight_count || 0} полетов, ${
          stats.operators_count || 0
        } операторов, ${stats.regions_count || 0} регионов`,
        {
          x: getCenterX(BODY_W), 
          y: yPos,
          w: BODY_W,
          h: 0.3,
          ...styles.body,
        }
      );
      yPos += 0.4;
    });
  }

  const topRegionMonthly =
    monthlyData.regions &&
    monthlyData.regions[0] &&
    monthlyData.regions[0].monthly_stats;
  if (
    topRegionMonthly &&
    Array.isArray(topRegionMonthly) &&
    topRegionMonthly.length
  ) {
    try {
      const labels = topRegionMonthly.map((m) =>
        safeText(m.month).slice(0, 12)
      );
      const values = topRegionMonthly.map((m) => m.flight_count || 0);
      if (values.some((v) => v > 0)) {
        const chartSlide = pptx.addSlide();
        chartSlide.background = { color: bgColor };
        const CHART_HEADING_W = 9;
        const CHART_W = 9;

        chartSlide.addText("Активность по месяцам (топ регион)", {
          x: getCenterX(CHART_HEADING_W), 
          y: 0.8,
          w: CHART_HEADING_W,
          h: 0.5,
          ...styles.heading,
        });

        const dataSeries = [{ name: "Полетов", labels, values }];
        // График
        chartSlide.addChart(pptx.ChartType.line, dataSeries, {
          x: getCenterX(CHART_W), 
          y: 1.6,
          w: CHART_W,
          h: 4,
          showLegend: false,
          showTitle: false,
          catAxisLabelFontFace: "Arial",
        });
      } else {
        chartSlideSafeTextAdd(
          slide,
          topRegionMonthly.slice(0, 6),
          styles,
          yPos
        );
      }
    } catch (e) {
      console.warn("Не удалось добавить месячный график:", e);
    }
  }
}

function chartSlideSafeTextAdd(slide, monthsArr, styles, startY) {
  const SUBHEADING_W = 9;
  const BODY_W = 9;

  slide.addText("Активность по месяцам (топ регион):", {
    x: getCenterX(SUBHEADING_W), 
    y: startY,
    w: SUBHEADING_W,
    h: 0.3,
    ...styles.highlight,
  });
  let y = startY + 0.4;
  monthsArr.forEach((m) => {
    slide.addText(`${m.month}: ${m.flight_count || 0} полетов`, {
      x: getCenterX(BODY_W), 
      y,
      w: BODY_W,
      h: 0.3,
      ...styles.body,
    });
    y += 0.35;
  });
}

function addTechParamsSlide(pptx, { dashboardData, styles, bgColor }) {
  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const HEADING_W = 9;
  const SUBHEADING_W = 9;
  const BODY_W = 9;

  // Заголовок слайда
  slide.addText("Технические параметры полетов", {
    x: getCenterX(HEADING_W), 
    y: 0.8,
    w: HEADING_W,
    h: 0.6,
    ...styles.heading,
  });

  let yPos = 1.8;
  if (dashboardData.level_stats) {
    const level = dashboardData.level_stats;
    // Подзаголовок
    slide.addText("Высота полета:", {
      x: getCenterX(SUBHEADING_W), 
      y: yPos,
      w: SUBHEADING_W,
      h: 0.4,
      ...styles.highlight,
    });
    yPos += 0.5;

    if (level.avg_level) {
      // Элемент списка
      slide.addText(`• Средняя: ${Math.round(level.avg_level)} м`, {
        x: getCenterX(BODY_W), 
        y: yPos,
        w: BODY_W,
        h: 0.3,
        ...styles.body,
      });
      yPos += 0.4;
    }
    if (level.max_level) {
      // Элемент списка
      slide.addText(`• Максимальная: ${Math.round(level.max_level)} м`, {
        x: getCenterX(BODY_W), 
        y: yPos,
        w: BODY_W,
        h: 0.3,
        ...styles.body,
      });
      yPos += 0.4;
    }
    if (level.most_common_level) {
      // Элемент списка
      slide.addText(`• Наиболее частая: ${level.most_common_level}`, {
        x: getCenterX(BODY_W), 
        y: yPos,
        w: BODY_W,
        h: 0.3,
        ...styles.body,
      });
      yPos += 0.4;
    }
  }

  yPos += 0.2;
  if (dashboardData.radius_stats) {
    const radius = dashboardData.radius_stats;
    // Подзаголовок
    slide.addText("Радиус полета:", {
      x: getCenterX(SUBHEADING_W), 
      y: yPos,
      w: SUBHEADING_W,
      h: 0.4,
      ...styles.highlight,
    });
    yPos += 0.5;

    if (radius.avg_radius) {
      // Элемент списка
      slide.addText(
        `• Средний: ${Math.round(radius.avg_radius)} м (${(
          radius.avg_radius / 1000
        ).toFixed(1)} км)`,
        {
          x: getCenterX(BODY_W), 
          y: yPos,
          w: BODY_W,
          h: 0.3,
          ...styles.body,
        }
      );
      yPos += 0.4;
    }
    if (radius.max_radius) {
      // Элемент списка
      slide.addText(
        `• Максимальный: ${Math.round(radius.max_radius)} м (${(
          radius.max_radius / 1000
        ).toFixed(1)} км)`,
        {
          x: getCenterX(BODY_W), 
          y: yPos,
          w: BODY_W,
          h: 0.3,
          ...styles.body,
        }
      );
    }
  }
}

function addConclusionSlide(pptx, { dashboardData, styles, bgColor }) {
  const slide = pptx.addSlide();
  slide.background = { color: bgColor };
  const TITLE_W = 9;
  const BULLET_W = 9;
  const SUBTITLE_W = 9;

  // Главный заголовок
  slide.addText("Резюме отчета", {
    x: getCenterX(TITLE_W), 
    y: 1.2,
    w: TITLE_W,
    h: 1,
    ...styles.title,
  });

  const generalStats = dashboardData.general_stats || {};
  const bullets = [
    `Всего выполнено полетов: ${generalStats.total_flights || 0}`,
    `География: ${generalStats.total_regions || 0} регионов`,
    `Участники: ${generalStats.total_operators || 0} операторов`,
    `Парк БПЛА: ${generalStats.total_aircrafts || 0} единиц техники`,
  ];

  if (dashboardData.active_region?.region) {
    bullets.push(
      `Наиболее активный регион: ${dashboardData.active_region.region} (${
        dashboardData.active_region.flight_count || 0
      } полетов)`
    );
  }

  let yPos = 2.5;
  bullets.forEach((bullet) => {
    // Элементы списка
    slide.addText(`• ${bullet}`, {
      x: getCenterX(BULLET_W), 
      y: yPos,
      w: BULLET_W,
      h: 0.4,
      fontSize: 16,
      color: "#2D3748",
      align: "center",
    });
    yPos += 0.45;
  });

  // Нижний колонтитул
  slide.addText("Спасибо за внимание!", {
    x: getCenterX(SUBTITLE_W), 
    y: 5.5,
    w: SUBTITLE_W,
    h: 0.6,
    ...styles.subtitle,
  });
}