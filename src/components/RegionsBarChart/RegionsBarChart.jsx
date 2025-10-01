// components/RegionsBarChart.jsx
import React, { useEffect, useRef, useCallback, memo } from 'react';
import * as d3 from 'd3';
import styles from './RegionsBarChart.module.css';

const RegionsBarChart = () => {
  const svgRef = useRef();
  const tooltipRef = useRef();

  // Мемоизируем все функции
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
  }, []);

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
  }, []);

  const renderChart = useCallback((data) => {
    if (!data || data.length === 0) {
      renderEmptyState();
      return;
    }

    const svg = d3.select(svgRef.current);
    const tooltip = d3.select(tooltipRef.current);
    
    // Очищаем предыдущий график
    svg.selectAll("*").remove();

    // Сортируем данные по количеству полетов
    const sortedData = [...data].sort((a, b) => b.num_flights - a.num_flights);
    
    // Увеличиваем нижний отступ для большего расстояния до легенды
    const margin = { top: 40, right: 30, bottom: 100, left: 150 };
    const width = svgRef.current.clientWidth - margin.left - margin.right;
    const height = svgRef.current.clientHeight - margin.top - margin.bottom;

    // Создаем основной контейнер
    const g = svg.append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // Шкалы
    const xScale = d3.scaleLinear()
      .domain([0, d3.max(sortedData, d => d.num_flights)])
      .range([0, width])
      .nice();

    const yScale = d3.scaleBand()
      .domain(sortedData.map(d => d.region || 'Не указан'))
      .range([0, height])
      .padding(0.2);

    // Оси
    const xAxis = d3.axisBottom(xScale)
      .ticks(Math.min(6, d3.max(sortedData, d => d.num_flights)))
      .tickFormat(d => d);

    const yAxis = d3.axisLeft(yScale)
      .tickSize(0);

    // Добавляем оси
    g.append("g")
      .attr("class", "x-axis")
      .attr("transform", `translate(0,${height})`)
      .call(xAxis)
      .call(g => g.select(".domain").remove())
      .call(g => g.selectAll(".tick line").clone()
        .attr("y2", -height)
        .attr("stroke-opacity", 0.1))
      .call(g => g.append("text")
        .attr("x", width)
        .attr("y", 30)
        .attr("fill", "currentColor")
        .attr("text-anchor", "end")
        .text("Количество полетов"));

    g.append("g")
      .attr("class", "y-axis")
      .call(yAxis)
      .call(g => g.select(".domain").remove())
      .call(g => g.selectAll(".tick text")
        .attr("dx", "-0.5em")
        .attr("text-anchor", "end")
        .attr("fill", "currentColor"));

    // Цветовая шкала для среднего времени полета
    const maxDuration = d3.max(sortedData, d => d.avg_flight_duration) || 1;
    const colorScale = d3.scaleSequential(d3.interpolateBlues)
      .domain([0, maxDuration]);

    // Барчики
    const bars = g.selectAll(".bar")
      .data(sortedData)
      .enter()
      .append("g")
      .attr("class", "bar-group");

    bars.append("rect")
      .attr("class", "bar")
      .attr("x", 0)
      .attr("y", d => yScale(d.region || 'Не указан'))
      .attr("width", d => xScale(d.num_flights))
      .attr("height", yScale.bandwidth())
      .attr("fill", d => colorScale(d.avg_flight_duration || 0))
      .attr("rx", 3)
      .attr("ry", 3)
      .on("mouseover", function(event, d) {
        // Подсветка при наведении
        d3.select(this).attr("fill", d3.color(colorScale(d.avg_flight_duration || 0)).brighter(0.5));
        
        // Показываем тултип
        tooltip
          .style("opacity", 1)
          .html(`
            <strong>${d.region || 'Не указан'}</strong><br/>
            Полетов: ${d.num_flights}<br/>
            Среднее время: ${(d.avg_flight_duration || 0).toFixed(1)} мин
          `);
      })
      .on("mousemove", function(event) {
        tooltip
          .style("left", (event.pageX + 10) + "px")
          .style("top", (event.pageY - 10) + "px");
      })
      .on("mouseout", function(event, d) {
        d3.select(this).attr("fill", colorScale(d.avg_flight_duration || 0));
        tooltip.style("opacity", 0);
      });

    // Добавляем значения на барчики
    bars.append("text")
      .attr("class", "bar-value")
      .attr("x", d => xScale(d.num_flights) + 5)
      .attr("y", d => yScale(d.region || 'Не указан') + yScale.bandwidth() / 2)
      .attr("dy", "0.35em")
      .attr("text-anchor", "start")
      .attr("fill", "currentColor")
      .attr("font-size", "12px")
      .attr("font-weight", "500")
      .text(d => d.num_flights);

    // Заголовок
    svg.append("text")
      .attr("x", margin.left + width / 2)
      .attr("y", 20)
      .attr("text-anchor", "middle")
      .attr("fill", "currentColor")
      .attr("font-size", "16px")
      .attr("font-weight", "600")
      // .text("Статистика полетов по регионам");

    // Легенда для среднего времени (только если есть данные о времени)
    if (maxDuration > 0) {
      const legendData = [0, maxDuration / 3, (2 * maxDuration) / 3, maxDuration];
      const legendWidth = 200;
      const legendHeight = 20;

      // Увеличиваем расстояние от графика до легенды
      const legendTopMargin = 60;
      const legend = svg.append("g")
        .attr("transform", `translate(${margin.left + width - legendWidth}, ${height + margin.top + legendTopMargin})`);

      const legendScale = d3.scaleLinear()
        .domain([0, maxDuration])
        .range([0, legendWidth]);

      // Создаем градиент для цветовой шкалы
      const defs = svg.append("defs");
      const gradient = defs.append("linearGradient")
        .attr("id", "legend-gradient")
        .attr("x1", "0%")
        .attr("x2", "100%")
        .attr("y1", "0%")
        .attr("y2", "0%");

      gradient.selectAll("stop")
        .data(legendData.map((d, i) => ({
          offset: `${(i / (legendData.length - 1)) * 100}%`,
          color: colorScale(d)
        })))
        .enter()
        .append("stop")
        .attr("offset", d => d.offset)
        .attr("stop-color", d => d.color);

      // Добавляем цветовую полосу
      legend.append("rect")
        .attr("width", legendWidth)
        .attr("height", legendHeight)
        .style("fill", "url(#legend-gradient)")
        .attr("rx", 3)
        .attr("ry", 3);

      // Добавляем заголовок легенды выше цветовой полосы
      legend.append("text")
        .attr("x", 0)
        .attr("y", -8)
        .attr("fill", "currentColor")
        .attr("font-size", "12px")
        .attr("font-weight", "500")
        .text("Среднее время полета");

      // Создаем ось легенды с отступами для текста
      const legendAxis = d3.axisBottom(legendScale)
        .ticks(4)
        .tickFormat(d => d.toFixed(0) + " мин");

      // Добавляем ось легенды под цветовой полосой
      const legendAxisGroup = legend.append("g")
        .attr("transform", `translate(0, ${legendHeight + 5})`) // Отступ под полосой
        .call(legendAxis);

      // Убираем линию оси чтобы не мешала
      legendAxisGroup.select(".domain").remove();

      // Убедимся, что текст виден
      legendAxisGroup.selectAll(".tick text")
        .attr("fill", "currentColor")
        .attr("font-size", "11px")
        .attr("dy", "0.5em");
    }
  }, [renderEmptyState]);

  // Мемоизируем основную функцию загрузки и рендеринга
  const fetchDataAndRender = useCallback(async () => {
    try {
      const response = await fetch('http://localhost:8000/stats/regions');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (!data || data.length === 0) {
        renderEmptyState();
        return;
      }
      
      renderChart(data);
    } catch (error) {
      console.error('Ошибка загрузки данных:', error);
      renderErrorState(error.message);
    }
  }, [renderEmptyState, renderChart, renderErrorState]);

  useEffect(() => {
    fetchDataAndRender();

    // Обработчик ресайза
    const handleResize = () => {
      if (svgRef.current) {
        fetchDataAndRender();
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [fetchDataAndRender]);

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
};

// Обертываем компонент в memo для предотвращения лишних ререндеров
export default memo(RegionsBarChart);