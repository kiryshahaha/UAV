// components/RegionBarChart.jsx
import React, { useEffect, useRef, useCallback, memo } from 'react';
import * as d3 from 'd3';
import styles from './RegionBarChart.module.css';

const RegionBarChart = memo(({ regionName, onLoad, onStartLoading }) => {
  const svgRef = useRef();
  const tooltipRef = useRef();

  // Добавляем колбэк начала загрузки
  useEffect(() => {
    if (onStartLoading) {
      onStartLoading();
    }
  }, [onStartLoading]);

  const renderEmptyState = useCallback(() => {
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    
    const width = svgRef.current.clientWidth;
    const height = svgRef.current.clientHeight;
    
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "16px")
      .text("Нет данных для отображения");
    
    // Уведомляем о завершении загрузки
    if (onLoad) {
      onLoad();
    }
  }, [onLoad]);

  const renderErrorState = useCallback((errorMessage) => {
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    
    const width = svgRef.current.clientWidth;
    const height = svgRef.current.clientHeight;
    
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 - 10)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "16px")
      .text("Ошибка загрузки данных");
      
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 + 20)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "12px")
      .text(errorMessage);
    
    // Уведомляем о завершении загрузки
    if (onLoad) {
      onLoad();
    }
  }, [onLoad]);

  const renderRegionNotFound = useCallback(() => {
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    
    const width = svgRef.current.clientWidth;
    const height = svgRef.current.clientHeight;
    
    // Основное сообщение
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 - 20)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "16px")
      .attr("font-weight", "600")
      .text("Информация по региону не найдена");
    
    // Название региона
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 + 10)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "14px")
      .text(`Регион: ${regionName}`);
    
    // Дополнительное пояснение
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2 + 40)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "12px")
      .attr("opacity", 0.7)
      .text("Нет данных о полетах за выбранный период");
    
    // Уведомляем о завершении загрузки
    if (onLoad) {
      onLoad();
    }
  }, [regionName, onLoad]);

  const renderChart = useCallback((data) => {
    if (!data || !data.monthly_stats || data.monthly_stats.length === 0) {
      renderEmptyState();
      return;
    }

    const svg = d3.select(svgRef.current);
    const tooltip = d3.select(tooltipRef.current);
    
    // Очищаем предыдущий график
    svg.selectAll("*").remove();

    const monthlyData = data.monthly_stats;
    
    // Настройки графика
    const margin = { top: 60, right: 30, bottom: 80, left: 60 };
    const width = svgRef.current.clientWidth - margin.left - margin.right;
    const height = svgRef.current.clientHeight - margin.top - margin.bottom;

    // Создаем основной контейнер
    const g = svg.append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // Шкалы
    const xScale = d3.scaleBand()
      .domain(monthlyData.map(d => d.month))
      .range([0, width])
      .padding(0.2);

    const yScale = d3.scaleLinear()
      .domain([0, d3.max(monthlyData, d => d.flight_count)])
      .range([height, 0])
      .nice();

    // Оси
    const xAxis = d3.axisBottom(xScale);
    const yAxis = d3.axisLeft(yScale)
      .ticks(Math.min(8, d3.max(monthlyData, d => d.flight_count)));

    // Добавляем оси
    g.append("g")
      .attr("class", "x-axis")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis)
      .selectAll("text")
      .attr("transform", "rotate(-45)")
      .attr("text-anchor", "end")
      .attr("dx", "-0.8em")
      .attr("dy", "0.15em")
      .attr("fill", "currentColor");

    g.append("g")
      .attr("class", "y-axis")
      .call(yAxis)
      .call(g => g.select(".domain").remove())
      .call(g => g.selectAll(".tick text")
        .attr("fill", "currentColor"));

    // Добавляем подписи осей
    g.append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", -margin.left + 15)
      .attr("x", -height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "12px")
      .text("Количество полетов");

    g.append("text")
      .attr("x", width / 2)
      .attr("y", height + margin.bottom - 10)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "12px")
      .text("Месяцы");

    // Цветовая шкала
    const colorScale = d3.scaleSequential(d3.interpolateBlues)
      .domain([0, d3.max(monthlyData, d => d.flight_count)]);

    // Барчики
    g.selectAll(".bar")
      .data(monthlyData)
      .enter()
      .append("rect")
      .attr("class", "bar")
      .attr("x", d => xScale(d.month))
      .attr("y", d => yScale(d.flight_count))
      .attr("width", xScale.bandwidth())
      .attr("height", d => height - yScale(d.flight_count))
      .attr("fill", d => colorScale(d.flight_count))
      .attr("rx", 3)
      .attr("ry", 3)
      .on("mouseover", function(event, d) {
        // Подсветка при наведении
        d3.select(this).attr("fill", d3.color(colorScale(d.flight_count)).brighter(0.5));
        
        // Показываем тултип
        tooltip
          .style("opacity", 1)
          .html(`
            <strong>${d.month}</strong><br/>
            Полетов: ${d.flight_count}
          `);
      })
      .on("mousemove", function(event) {
        tooltip
          .style("left", (event.pageX + 10) + "px")
          .style("top", (event.pageY - 10) + "px");
      })
      .on("mouseout", function(event, d) {
        d3.select(this).attr("fill", colorScale(d.flight_count));
        tooltip.style("opacity", 0);
      });

    // Добавляем значения на барчики
    g.selectAll(".bar-value")
      .data(monthlyData)
      .enter()
      .append("text")
      .attr("class", "bar-value")
      .attr("x", d => xScale(d.month) + xScale.bandwidth() / 2)
      .attr("y", d => yScale(d.flight_count) - 5)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "11px")
      .attr("font-weight", "500")
      .text(d => d.flight_count > 0 ? d.flight_count : "");

    // Заголовок с названием региона
    svg.append("text")
      .attr("x", margin.left + width / 2)
      .attr("y", 20)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "16px")
      .attr("font-weight", "600")
      .text(`Статистика полетов - ${regionName}`);

    // Общее количество полетов
    const totalFlights = monthlyData.reduce((sum, d) => sum + d.flight_count, 0);
    svg.append("text")
      .attr("x", margin.left + width / 2)
      .attr("y", 40)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "14px")
      .attr("font-weight", "500")
      .text(`Всего полетов: ${totalFlights}`);

    // Уведомляем о завершении загрузки
    if (onLoad) {
      onLoad();
    }

  }, [renderEmptyState, regionName, onLoad]);
  
  const fetchDataAndRender = useCallback(async () => {
    if (!regionName) {
      renderEmptyState();
      return;
    }

    try {
      // Сначала получаем общую статистику по регионам
      const response = await fetch('http://37.252.22.137:8000/stats/regions/monthly');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const allRegionsData = await response.json();
      
      if (!allRegionsData.regions || allRegionsData.regions.length === 0) {
        renderEmptyState();
        return;
      }
      
      // Ищем данные для конкретного региона
      const regionData = allRegionsData.regions.find(region => 
        region.region === regionName
      );

      if (!regionData) {
        // Используем новую функцию для отображения "регион не найден"
        renderRegionNotFound();
        return;
      }
      
      renderChart(regionData);
    } catch (error) {
      console.error('Ошибка загрузки данных региона:', error);
      renderErrorState(error.message);
    }
  }, [regionName, renderEmptyState, renderChart, renderErrorState, renderRegionNotFound]);

  useEffect(() => {
    fetchDataAndRender();

    const handleResize = () => {
      if (svgRef.current && regionName) {
        fetchDataAndRender();
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [fetchDataAndRender, regionName]);

  return (
    <div className={styles.container}>
      <svg 
        ref={svgRef} 
        className={styles.chart}
        width="100%" 
        height="100%"
      />
      <div 
        ref={tooltipRef} 
        className={styles.tooltip}
      />
    </div>
  );
});

RegionBarChart.displayName = 'RegionBarChart';
export default RegionBarChart;