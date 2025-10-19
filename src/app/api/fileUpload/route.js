// services/fileUploadService.js
export const uploadFile = async (file, year) => {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("year", year.toString());

  try {
    const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/upload`, {
      method: "POST",
      body: formData,
    });

    if (response.ok) {
      const result = await response.json();
      return { success: true, message: result.message || "Данные обработаны" };
    } else {
      const error = await response.json();
      return { success: false, message: error.detail || "Не удалось загрузить файл" };
    }
  } catch (error) {
    return { success: false, message: `Ошибка сети: ${error.message}` };
  }
};

export const generateYearOptions = () => {
  const currentYear = new Date().getFullYear();
  const years = [];
  for (let year = currentYear; year >= 2020; year--) {
    years.push(year);
  }
  return years;
};