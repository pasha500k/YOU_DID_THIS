import os
import json
from openai import OpenAI

# =========================================================
# Настройка:
# 1) Установи библиотеку:
#    pip install openai
#
# 2) Задай API-ключ:
#    Windows PowerShell:
#    $env:OPENAI_API_KEY="твой_ключ"
#
#    Windows CMD:
#    set OPENAI_API_KEY=твой_ключ
# =========================================================

MODEL = "gpt-5"

client = OpenAI(api_key='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')


def build_prompt(topic: str, school_class: str, tasks_per_variant: int) -> str:
    return f"""
Составь контрольную работу по теме: "{topic}".
Класс: {school_class}.

Нужно создать РОВНО 10 вариантов.
В КАЖДОМ варианте должно быть РОВНО {tasks_per_variant} заданий.

Требования:
1. Все варианты должны быть разными.
2. Уровень сложности должен соответствовать классу {school_class}.
3. Формулировки должны быть понятными школьнику.
4. Не добавляй ответы и решения.
5. Оформи аккуратно:
   ВАРИАНТ 1
   1.
   2.
   ...
   ВАРИАНТ 2
   ...
6. Только русский язык.
7. Не используй markdown, таблицы и лишние пояснения.
8. Начни сразу с "ВАРИАНТ 1".

Дополнительно:
- Старайся, чтобы задания внутри одного варианта были разнообразными.
- Если тема математическая, можно включать вычисления, уравнения, задачи.
- Если тема гуманитарная, можно включать теорию, анализ, краткие ответы.
""".strip()


def generate_variants(topic: str, school_class: str, tasks_per_variant: int) -> str:
    prompt = build_prompt(topic, school_class, tasks_per_variant)

    response = client.responses.create(
        model=MODEL,
        input=prompt
    )

    # В актуальном SDK текст обычно доступен через output_text
    return response.output_text.strip()


def save_to_file(text: str, filename: str = "variants.txt") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)


def main():
    print("Генератор 10 вариантов контрольной работы\n")

    topic = input("Введите тему: ").strip()
    school_class = input("Введите класс (например, 6 класс): ").strip()

    while True:
        tasks_input = input("Сколько заданий должно быть в каждом варианте? ").strip()
        try:
            tasks_per_variant = int(tasks_input)
            if tasks_per_variant <= 0:
                print("Введите число больше 0.")
                continue
            break
        except ValueError:
            print("Нужно ввести целое число.")

    print("\nГенерация...\n")

    try:
        result = generate_variants(topic, school_class, tasks_per_variant)
        print(result)
        save_to_file(result, "variants.txt")
        print("\nСохранено в файл: variants.txt")
    except Exception as e:
        print("Ошибка при генерации:")
        print(e)


if __name__ == "__main__":
    main()