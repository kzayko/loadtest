"""
Симуляция системы с очередью и тремя этапами обработки.
Этап 2 состоит из трех параллельных обработок.
"""

import simpy
import random
import numpy as np
import matplotlib.pyplot as plt
import json
import os
from collections import defaultdict
from dotenv import load_dotenv

# Загрузка параметров из .env файла
load_dotenv()

# Константы системы
INCOMING_RATE = int(os.getenv('INCOMING_RATE', 14))  # заявок/сек
STAGE1_WORKERS = int(os.getenv('STAGE1_WORKERS', 2000))
STAGE2_WORKERS = int(os.getenv('STAGE2_WORKERS', 1260))
STAGE3_WORKERS = int(os.getenv('STAGE3_WORKERS', 180))
STAGE1_TIME = int(os.getenv('STAGE1_TIME', 20))  # секунд
STAGE2_TIME = int(os.getenv('STAGE2_TIME', 30))  # секунд
STAGE3_TIME = int(os.getenv('STAGE3_TIME', 12))  # секунд

# Параметры симуляции
SIMULATION_TIME_HOURS = float(os.getenv('SIMULATION_TIME_HOURS', 12))  # Время симуляции в часах
SIMULATION_TIME = int(SIMULATION_TIME_HOURS * 3600)  # Конвертация в секунды
QUEUE_LIMIT = int(os.getenv('QUEUE_LIMIT', 10000))  # Лимит главной очереди (для отказов)

# Параметры графиков
FIGURE_WIDTH = float(os.getenv('FIGURE_WIDTH', 14))
FIGURE_HEIGHT_PER_SUBPLOT = float(os.getenv('FIGURE_HEIGHT_PER_SUBPLOT', 3.5))
SUBTITLE_FONTSIZE = int(os.getenv('SUBTITLE_FONTSIZE', 14))
AXIS_LABEL_FONTSIZE = int(os.getenv('AXIS_LABEL_FONTSIZE', 10))
TITLE_FONTSIZE = int(os.getenv('TITLE_FONTSIZE', 11))
LEGEND_FONTSIZE = int(os.getenv('LEGEND_FONTSIZE', 9))
PARAMS_TEXT_FONTSIZE = int(os.getenv('PARAMS_TEXT_FONTSIZE', 8))
H_PAD = float(os.getenv('H_PAD', 4.0))
TOP_MARGIN = float(os.getenv('TOP_MARGIN', 0.95))
BOTTOM_MARGIN = float(os.getenv('BOTTOM_MARGIN', 0.05))
DPI = int(os.getenv('DPI', 300))

class Request:
    """Класс для представления заявки"""
    def __init__(self, request_id, arrival_time):
        self.id = request_id
        self.arrival_time = arrival_time
        self.stage1_start = None
        self.stage1_end = None
        self.stage2_start = None
        self.stage2_end = None
        self.stage3_start = None
        self.stage3_end = None
        self.completion_time = None
        self.rejected = False


def request_generator(env, stage1_queue, stage2_queue, stage3_queue, queue_counters, processing_counters, stats):
    """Генератор входящих заявок"""
    request_id = 0
    while True:
        # Экспоненциальное распределение для интервалов между заявками
        yield env.timeout(random.expovariate(INCOMING_RATE))
        
        request = Request(request_id, env.now)
        request_id += 1
        
        # Проверка лимита главной очереди (общее количество заявок в системе)
        # Главная очередь = все заявки во всех очередях + все заявки в обработке
        total_in_system = (queue_counters['stage1'] + queue_counters['stage2'] + queue_counters['stage3'] +
                          processing_counters['stage1'] + processing_counters['stage2'] + processing_counters['stage3'])
        
        if total_in_system >= QUEUE_LIMIT:
            request.rejected = True
            current_minute = int(env.now // 60)
            stats[current_minute]['rejections'] += 1
        else:
            # Внутренние очереди нелимитированы, заявка всегда принимается
            stage1_queue.put(request)
            queue_counters['stage1'] += 1


def stage1_processor(env, stage1_queue, stage2_queue, queue_counters, processing_counters, stats):
    """Обработчик первого этапа"""
    while True:
        request = yield stage1_queue.get()
        queue_counters['stage1'] -= 1  # Заявка извлечена из очереди
        processing_counters['stage1'] += 1  # Заявка начала обработку
        request.stage1_start = env.now
        
        # Обработка первого этапа
        yield env.timeout(STAGE1_TIME)
        
        processing_counters['stage1'] -= 1  # Обработка завершена
        request.stage1_end = env.now
        
        # Передача на второй этап
        stage2_queue.put(request)
        queue_counters['stage2'] += 1


def stage2_processor(env, stage2_queue, stage3_queue, queue_counters, processing_counters, stats):
    """Обработчик второго этапа (требует три параллельные обработки)"""
    while True:
        request = yield stage2_queue.get()
        queue_counters['stage2'] -= 1  # Заявка извлечена из очереди
        request.stage2_start = env.now
        
        # Время ожидания в очереди этапа 2 (до получения ресурса)
        wait_start = env.now
        
        # Три параллельные обработки для этапа 2
        # Каждая заявка требует 3 единицы ресурса одновременно
        # Используем Container для моделирования ресурса с количеством
        yield stage2_resource.get(amount=3)
        
        # Время ожидания ресурса на этапе 2
        wait_time_stage2 = env.now - wait_start
        
        # Общее время ожидания заявки во всех очередях системы
        # От момента поступления до начала обработки на этапе 2
        total_wait_time = env.now - request.arrival_time
        
        processing_counters['stage2'] += 1  # Заявка начала обработку
        
        # Все три обработки выполняются параллельно в течение STAGE2_TIME
        yield env.timeout(STAGE2_TIME)
        
        # Освобождаем ресурс
        yield stage2_resource.put(amount=3)
        processing_counters['stage2'] -= 1  # Обработка завершена
        
        request.stage2_end = env.now
        
        # Сохранение времени ожидания
        current_minute = int(env.now // 60)
        stats[current_minute]['wait_time'].append(total_wait_time)
        
        # Передача на третий этап
        stage3_queue.put(request)
        queue_counters['stage3'] += 1


def stage3_processor(env, stage3_queue, queue_counters, processing_counters, stats):
    """Обработчик третьего этапа"""
    while True:
        request = yield stage3_queue.get()
        queue_counters['stage3'] -= 1  # Заявка извлечена из очереди
        processing_counters['stage3'] += 1  # Заявка начала обработку
        request.stage3_start = env.now
        
        # Обработка третьего этапа
        yield env.timeout(STAGE3_TIME)
        
        processing_counters['stage3'] -= 1  # Обработка завершена
        request.stage3_end = env.now
        request.completion_time = env.now


def statistics_collector(env, stage1_queue, stage2_queue, stage3_queue, queue_counters, processing_counters, stats, minute_log):
    """Сбор статистики поминутно"""
    last_logged_minute = -1
    
    while True:
        current_time = env.now
        current_minute = int(current_time // 60)
        
        # Главная очередь - общее количество заявок в системе (все этапы)
        total_in_system = (queue_counters['stage1'] + queue_counters['stage2'] + queue_counters['stage3'] +
                          processing_counters['stage1'] + processing_counters['stage2'] + processing_counters['stage3'])
        
        # Расчет ожидаемого времени ожидания для заявки, пришедшей в этот момент
        # Пропускная способность каждого этапа
        stage1_throughput = STAGE1_WORKERS / STAGE1_TIME  # заявок/сек
        stage2_throughput = (STAGE2_WORKERS / 3) / STAGE2_TIME  # заявок/сек (каждая заявка требует 3 единицы ресурса)
        stage3_throughput = STAGE3_WORKERS / STAGE3_TIME  # заявок/сек
        
        # Ожидаемое время ожидания в очереди этапа 1
        expected_wait_stage1 = queue_counters['stage1'] / stage1_throughput if stage1_throughput > 0 else 0
        
        # Ожидаемое время ожидания в очереди этапа 2
        # Учитываем только заявки в очереди этапа 2
        # Заявки в обработке на этапе 2 не учитываем, так как они уже начали обработку
        # и не влияют на время ожидания новой заявки
        expected_wait_stage2 = queue_counters['stage2'] / stage2_throughput if stage2_throughput > 0 else 0
        
        # Общее ожидаемое время ожидания = время ожидания в очереди этапа 1 + 
        # время ожидания в очереди этапа 2
        # Это время ожидания в очередях до начала обработки, без учета времени самой обработки
        expected_total_wait = expected_wait_stage1 + expected_wait_stage2
        
        # Сохраняем статистику для всех очередей
        stats[current_minute]['main_queue'].append(total_in_system)  # Главная очередь
        stats[current_minute]['stage1_queue'].append(queue_counters['stage1'])  # Очередь этапа 1
        stats[current_minute]['stage2_queue'].append(queue_counters['stage2'])  # Очередь этапа 2
        stats[current_minute]['stage3_queue'].append(queue_counters['stage3'])  # Очередь этапа 3
        # Сохраняем время ожидания в минутах
        stats[current_minute]['expected_wait_time'].append(expected_total_wait / 60.0)  # Ожидаемое время ожидания в минутах
        stats[current_minute]['time'].append(current_time)
        
        # Логирование поминутных отчетов
        if current_minute > last_logged_minute:
            # Логируем данные предыдущей минуты (если она была и имеет данные)
            if last_logged_minute >= 0 and stats[last_logged_minute]['main_queue']:
                prev_minute = last_logged_minute
                avg_main = np.mean(stats[prev_minute]['main_queue'])
                avg_stage1 = np.mean(stats[prev_minute]['stage1_queue'])
                avg_stage2 = np.mean(stats[prev_minute]['stage2_queue'])
                avg_stage3 = np.mean(stats[prev_minute]['stage3_queue'])
                avg_wait = np.mean(stats[prev_minute]['expected_wait_time'])
                
                minute_log.append({
                    'minute': prev_minute,
                    'main_queue_avg': float(avg_main),
                    'stage1_queue_avg': float(avg_stage1),
                    'stage2_queue_avg': float(avg_stage2),
                    'stage3_queue_avg': float(avg_stage3),
                    'expected_wait_time_avg': float(avg_wait),
                    'rejections': stats[prev_minute]['rejections']
                })
            last_logged_minute = current_minute
        
        yield env.timeout(1)  # Обновление каждую секунду


def run_simulation():
    """Запуск симуляции"""
    env = simpy.Environment()
    
    # Очереди для каждого этапа
    stage1_queue = simpy.Store(env, capacity=float('inf'))
    stage2_queue = simpy.Store(env, capacity=float('inf'))
    stage3_queue = simpy.Store(env, capacity=float('inf'))
    
    # Ресурс для этапа 2 (ограничение параллельных обработок)
    # Используем Container, так как каждая заявка требует 3 единицы ресурса
    global stage2_resource
    stage2_resource = simpy.Container(env, capacity=STAGE2_WORKERS, init=STAGE2_WORKERS)
    
    # Счетчики для отслеживания размера очередей (ожидающие обработки)
    queue_counters = {
        'stage1': 0,
        'stage2': 0,
        'stage3': 0
    }
    
    # Счетчики для отслеживания заявок в обработке
    processing_counters = {
        'stage1': 0,
        'stage2': 0,
        'stage3': 0
    }
    
    # Статистика
    stats = defaultdict(lambda: {
        'main_queue': [],      # Главная очередь (все заявки в системе)
        'stage1_queue': [],    # Очередь этапа 1
        'stage2_queue': [],    # Очередь этапа 2
        'stage3_queue': [],    # Очередь этапа 3
        'expected_wait_time': [],  # Ожидаемое время ожидания для новой заявки
        'wait_time': [],       # Фактическое время ожидания (для справки)
        'rejections': 0,
        'time': []
    })
    
    # Лог поминутных отчетов
    minute_log = []
    
    # Запуск генератора заявок
    env.process(request_generator(env, stage1_queue, stage2_queue, stage3_queue, queue_counters, processing_counters, stats))
    
    # Запуск обработчиков первого этапа
    for _ in range(STAGE1_WORKERS):
        env.process(stage1_processor(env, stage1_queue, stage2_queue, queue_counters, processing_counters, stats))
    
    # Запуск обработчиков второго этапа
    for _ in range(STAGE2_WORKERS):
        env.process(stage2_processor(env, stage2_queue, stage3_queue, queue_counters, processing_counters, stats))
    
    # Запуск обработчиков третьего этапа
    for _ in range(STAGE3_WORKERS):
        env.process(stage3_processor(env, stage3_queue, queue_counters, processing_counters, stats))
    
    # Сбор статистики
    env.process(statistics_collector(env, stage1_queue, stage2_queue, stage3_queue, queue_counters, processing_counters, stats, minute_log))
    
    # Запуск симуляции
    print("Starting simulation...")
    print(f"Incoming rate: {INCOMING_RATE} requests/sec")
    print(f"Stage 1 throughput: {STAGE1_WORKERS / STAGE1_TIME:.2f} requests/sec")
    print(f"Stage 2 throughput: {STAGE2_WORKERS / 3 / STAGE2_TIME:.2f} requests/sec (each request requires 3 resource units)")
    print(f"Stage 3 throughput: {STAGE3_WORKERS / STAGE3_TIME:.2f} requests/sec")
    print(f"Simulation time: {SIMULATION_TIME_HOURS} hours")
    env.run(until=SIMULATION_TIME)
    print("Simulation completed")
    
    # Логирование последней минуты
    last_minute = int(SIMULATION_TIME // 60) - 1
    if last_minute >= 0 and stats[last_minute]['main_queue']:
        avg_main = np.mean(stats[last_minute]['main_queue'])
        avg_stage1 = np.mean(stats[last_minute]['stage1_queue'])
        avg_stage2 = np.mean(stats[last_minute]['stage2_queue'])
        avg_stage3 = np.mean(stats[last_minute]['stage3_queue'])
        avg_wait = np.mean(stats[last_minute]['expected_wait_time'])
        
        minute_log.append({
            'minute': last_minute,
            'main_queue_avg': float(avg_main),
            'stage1_queue_avg': float(avg_stage1),
            'stage2_queue_avg': float(avg_stage2),
            'stage3_queue_avg': float(avg_stage3),
            'expected_wait_time_avg': float(avg_wait),
            'rejections': stats[last_minute]['rejections']
        })
    
    return stats, minute_log


def generate_filename():
    """Генерация имени файла на основе параметров"""
    # Формат: количество_запросов_обработчики1_обработчики2_обработчики3
    filename = f"{INCOMING_RATE}_{STAGE1_WORKERS}_{STAGE2_WORKERS}_{STAGE3_WORKERS}"
    return filename


def save_simulation_log(minute_log, filename):
    """Сохранение лога симуляции в JSON файл"""
    log_data = {
        'simulation_parameters': {
            'incoming_rate': INCOMING_RATE,
            'stage1_workers': STAGE1_WORKERS,
            'stage1_time': STAGE1_TIME,
            'stage2_workers': STAGE2_WORKERS,
            'stage2_time': STAGE2_TIME,
            'stage3_workers': STAGE3_WORKERS,
            'stage3_time': STAGE3_TIME,
            'queue_limit': QUEUE_LIMIT,
            'simulation_time_seconds': SIMULATION_TIME,
            'simulation_time_hours': SIMULATION_TIME_HOURS
        },
        'minute_reports': minute_log
    }
    
    json_filename = f"{filename}.json"
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    
    print(f"Simulation log saved to file '{json_filename}'")
    return json_filename


def calculate_figure_size(num_subplots, height_per_subplot=None):
    """
    Расчет размера фигуры с учетом количества графиков и подписей осей
    
    Args:
        num_subplots: количество графиков
        height_per_subplot: высота на один график (по умолчанию из параметров)
    
    Returns:
        tuple: (width, height) размер фигуры
    """
    if height_per_subplot is None:
        height_per_subplot = FIGURE_HEIGHT_PER_SUBPLOT
    
    # Базовая высота для заголовка и отступов
    base_height = 1.5
    
    # Высота для каждого графика с учетом подписей осей и заголовков
    subplot_height = height_per_subplot
    
    # Общая высота
    total_height = base_height + (num_subplots * subplot_height)
    
    return (FIGURE_WIDTH, total_height)


def plot_statistics(stats, filename):
    """Построение поминутных графиков"""
    # Настройка для отображения русских шрифтов
    plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False
    
    # Количество графиков
    NUM_SUBPLOTS = 5
    
    # Подготовка данных для графиков
    minutes = sorted([m for m in stats.keys() if m < SIMULATION_TIME // 60])
    
    avg_main_queue = []
    avg_stage1_queue = []
    avg_stage2_queue = []
    avg_stage3_queue = []
    max_stage1_queue = []
    max_stage2_queue = []
    max_stage3_queue = []
    avg_expected_wait_times = []
    total_rejections = []
    minute_labels = []
    
    for minute in minutes:
        # Главная очередь
        if stats[minute]['main_queue']:
            avg_main_queue.append(np.mean(stats[minute]['main_queue']))
        else:
            avg_main_queue.append(0)
        
        # Очереди этапов (средние значения)
        if stats[minute]['stage1_queue']:
            avg_stage1_queue.append(np.mean(stats[minute]['stage1_queue']))
            max_stage1_queue.append(np.max(stats[minute]['stage1_queue']))
        else:
            avg_stage1_queue.append(0)
            max_stage1_queue.append(0)
            
        if stats[minute]['stage2_queue']:
            avg_stage2_queue.append(np.mean(stats[minute]['stage2_queue']))
            max_stage2_queue.append(np.max(stats[minute]['stage2_queue']))
        else:
            avg_stage2_queue.append(0)
            max_stage2_queue.append(0)
            
        if stats[minute]['stage3_queue']:
            avg_stage3_queue.append(np.mean(stats[minute]['stage3_queue']))
            max_stage3_queue.append(np.max(stats[minute]['stage3_queue']))
        else:
            avg_stage3_queue.append(0)
            max_stage3_queue.append(0)
        
        # Ожидаемое время ожидания (для заявки, пришедшей в эту минуту)
        if stats[minute]['expected_wait_time']:
            # Берем среднее значение ожидаемого времени за минуту
            avg_expected_wait_time = np.mean(stats[minute]['expected_wait_time'])
            avg_expected_wait_times.append(avg_expected_wait_time)
        else:
            avg_expected_wait_times.append(0)
        
        total_rejections.append(stats[minute]['rejections'])
        minute_labels.append(minute)
    
    # Расчет размера фигуры с учетом количества графиков и подписей
    fig_width, fig_height = calculate_figure_size(NUM_SUBPLOTS)
    
    # Создание графиков
    fig, axes = plt.subplots(NUM_SUBPLOTS, 1, figsize=(fig_width, fig_height))
    fig.suptitle('Статистика системы очередей (поминутно)', fontsize=SUBTITLE_FONTSIZE, fontweight='bold')
    
    # Добавление параметров моделирования на график
    params_text = (
        f"Параметры моделирования:\n"
        f"Входящий поток: {INCOMING_RATE} заявок/сек\n"
        f"Этап 1: {STAGE1_WORKERS} обработчиков, {STAGE1_TIME}с время обработки\n"
        f"Этап 2: {STAGE2_WORKERS} единиц ресурса, {STAGE2_TIME}с время обработки (3 ед. на заявку)\n"
        f"Этап 3: {STAGE3_WORKERS} обработчиков, {STAGE3_TIME}с время обработки\n"
        f"Лимит очереди: {QUEUE_LIMIT} заявок\n"
        f"Время симуляции: {SIMULATION_TIME_HOURS} часов"
    )
    
    # Добавляем текстовый блок с параметрами в правый верхний угол первого графика
    axes[0].text(0.98, 0.98, params_text, 
                transform=axes[0].transAxes,
                fontsize=PARAMS_TEXT_FONTSIZE,
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                family='monospace')
    
    # График 1: Главная очередь (все заявки в системе)
    axes[0].plot(minute_labels, avg_main_queue, 'b-', linewidth=2, label='Главная очередь (всего в системе)')
    axes[0].axhline(y=QUEUE_LIMIT, color='r', linestyle='--', linewidth=1, label=f'Лимит очереди ({QUEUE_LIMIT})')
    axes[0].set_xlabel('Минута', fontsize=AXIS_LABEL_FONTSIZE)
    axes[0].set_ylabel('Количество заявок', fontsize=AXIS_LABEL_FONTSIZE)
    axes[0].set_title('Главная очередь - Всего заявок в системе (все этапы)', fontsize=TITLE_FONTSIZE)
    axes[0].legend(fontsize=LEGEND_FONTSIZE)
    axes[0].grid(True, alpha=0.3)
    axes[0].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)
    
    # График 2: Очереди на каждом этапе
    axes[1].plot(minute_labels, avg_stage1_queue, 'g-', linewidth=2, label='Очередь этапа 1')
    axes[1].plot(minute_labels, avg_stage2_queue, 'orange', linewidth=2, label='Очередь этапа 2')
    axes[1].plot(minute_labels, avg_stage3_queue, 'purple', linewidth=2, label='Очередь этапа 3')
    axes[1].set_xlabel('Минута', fontsize=AXIS_LABEL_FONTSIZE)
    axes[1].set_ylabel('Количество заявок', fontsize=AXIS_LABEL_FONTSIZE)
    axes[1].set_title('Внутренние очереди на каждом этапе', fontsize=TITLE_FONTSIZE)
    axes[1].legend(fontsize=LEGEND_FONTSIZE)
    axes[1].grid(True, alpha=0.3)
    axes[1].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)
    
    # График 3: Ожидаемое время ожидания
    axes[2].plot(minute_labels, avg_expected_wait_times, 'r-', linewidth=2)
    axes[2].set_xlabel('Минута', fontsize=AXIS_LABEL_FONTSIZE)
    axes[2].set_ylabel('Ожидаемое время ожидания (мин)', fontsize=AXIS_LABEL_FONTSIZE)
    axes[2].set_title('Ожидаемое время ожидания в очереди (для заявки, пришедшей в этот момент)', fontsize=TITLE_FONTSIZE)
    axes[2].grid(True, alpha=0.3)
    axes[2].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)
    
    # График 4: Максимальные очереди каждого этапа
    axes[3].plot(minute_labels, max_stage1_queue, 'g-', linewidth=2, label='Макс. очередь этапа 1')
    axes[3].plot(minute_labels, max_stage2_queue, 'orange', linewidth=2, label='Макс. очередь этапа 2')
    axes[3].plot(minute_labels, max_stage3_queue, 'purple', linewidth=2, label='Макс. очередь этапа 3')
    axes[3].set_xlabel('Минута', fontsize=AXIS_LABEL_FONTSIZE)
    axes[3].set_ylabel('Максимальное количество заявок', fontsize=AXIS_LABEL_FONTSIZE)
    axes[3].set_title('Максимальные очереди на каждом этапе', fontsize=TITLE_FONTSIZE)
    axes[3].legend(fontsize=LEGEND_FONTSIZE)
    axes[3].grid(True, alpha=0.3)
    axes[3].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)
    
    # График 5: Количество отказов
    axes[4].bar(minute_labels, total_rejections, color='red', alpha=0.7)
    axes[4].set_xlabel('Минута', fontsize=AXIS_LABEL_FONTSIZE)
    axes[4].set_ylabel('Количество отказов', fontsize=AXIS_LABEL_FONTSIZE)
    axes[4].set_title('Количество отказов (превышение лимита главной очереди)', fontsize=TITLE_FONTSIZE)
    axes[4].grid(True, alpha=0.3, axis='y')
    axes[4].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)
    
    # Используем tight_layout с параметрами из конфигурации
    plt.tight_layout(rect=[0, BOTTOM_MARGIN, 1, TOP_MARGIN], h_pad=H_PAD)
    png_filename = f"{filename}.png"
    plt.savefig(png_filename, dpi=DPI, bbox_inches='tight', pad_inches=0.2)
    print(f"\nGraphs saved to file '{png_filename}'")
    plt.show()
    
    # Вывод общей статистики
    print("\n=== Overall Statistics ===")
    print(f"Total rejections: {sum(total_rejections)}")
    print(f"\nMain queue (total in system):")
    print(f"  Average: {np.mean(avg_main_queue):.2f}")
    print(f"  Maximum: {max(avg_main_queue) if avg_main_queue else 0:.2f}")
    print(f"\nStage 1 queue:")
    print(f"  Average: {np.mean(avg_stage1_queue):.2f}")
    print(f"  Maximum (average): {max(avg_stage1_queue) if avg_stage1_queue else 0:.2f}")
    print(f"  Maximum (peak): {max(max_stage1_queue) if max_stage1_queue else 0:.2f}")
    print(f"\nStage 2 queue:")
    print(f"  Average: {np.mean(avg_stage2_queue):.2f}")
    print(f"  Maximum (average): {max(avg_stage2_queue) if avg_stage2_queue else 0:.2f}")
    print(f"  Maximum (peak): {max(max_stage2_queue) if max_stage2_queue else 0:.2f}")
    print(f"\nStage 3 queue:")
    print(f"  Average: {np.mean(avg_stage3_queue):.2f}")
    print(f"  Maximum (average): {max(avg_stage3_queue) if avg_stage3_queue else 0:.2f}")
    print(f"  Maximum (peak): {max(max_stage3_queue) if max_stage3_queue else 0:.2f}")
    print(f"\nExpected wait time (for new request):")
    print(f"  Average: {np.mean(avg_expected_wait_times):.2f} min")
    print(f"  Maximum: {max(avg_expected_wait_times) if avg_expected_wait_times else 0:.2f} min")


if __name__ == "__main__":
    # Установка seed для воспроизводимости
    random.seed(42)
    np.random.seed(42)
    
    # Генерация имени файла
    filename = generate_filename()
    print(f"Results filename: {filename}")
    
    # Запуск симуляции
    stats, minute_log = run_simulation()
    
    # Сохранение JSON лога
    save_simulation_log(minute_log, filename)
    
    # Построение графиков
    plot_statistics(stats, filename)
