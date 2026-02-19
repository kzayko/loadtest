"""
Симуляция системы с очередью и произвольным числом этапов обработки.
Конфигурация: GEN_Stages, GEN_Timers, GEN_Workers в .env (формат 1-3-1 ...).
"""

import simpy
import random
import numpy as np
import matplotlib.pyplot as plt
import json
import os
import argparse
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# --- Парсинг конфигурации этапов из .env ---
def _parse_int_list(env_key, default_str):
    s = os.getenv(env_key, default_str)
    return [int(x.strip()) for x in s.split('-') if x.strip()]

# Количество параллельных обработок на каждом этапе (единиц ресурса на заявку)
GEN_Stages = _parse_int_list('GEN_Stages', '1-3-1')
# Время обработки на каждом этапе (сек)
GEN_Timers = _parse_int_list('GEN_Timers', '20-30-12')
# Количество обработчиков (ресурса) на каждом этапе
GEN_Workers = _parse_int_list('GEN_Workers', '2000-1260-180')

NUM_STAGES = len(GEN_Stages)
if len(GEN_Timers) != NUM_STAGES or len(GEN_Workers) != NUM_STAGES:
    raise ValueError(
        f"GEN_Stages, GEN_Timers, GEN_Workers must have same length. "
        f"Got {len(GEN_Stages)}, {len(GEN_Timers)}, {len(GEN_Workers)}"
    )

INCOMING_RATE = int(os.getenv('INCOMING_RATE', 14))
QUEUE_LIMIT = int(os.getenv('QUEUE_LIMIT', 10000))

# Время симуляции: из .env по умолчанию 2 часа, переопределяется из командной строки
SIMULATION_TIME_HOURS = float(os.getenv('SIMULATION_TIME_HOURS', 2))
SIMULATION_TIME = int(SIMULATION_TIME_HOURS * 3600)

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
    """Заявка с произвольным числом этапов"""
    def __init__(self, request_id, arrival_time):
        self.id = request_id
        self.arrival_time = arrival_time
        self.stage_starts = [None] * NUM_STAGES
        self.stage_ends = [None] * NUM_STAGES
        self.completion_time = None
        self.rejected = False


def request_generator(env, queues, queue_counters, processing_counters, stats):
    request_id = 0
    while True:
        yield env.timeout(random.expovariate(INCOMING_RATE))
        request = Request(request_id, env.now)
        request_id += 1
        total_in_system = sum(queue_counters) + sum(processing_counters)
        if total_in_system >= QUEUE_LIMIT:
            request.rejected = True
            current_minute = int(env.now // 60)
            stats[current_minute]['rejections'] += 1
        else:
            queues[0].put(request)
            queue_counters[0] += 1


def stage_processor(env, stage_idx, in_queue, out_queue, queue_counters, processing_counters,
                    stats, resources, generation_times):
    """Универсальный обработчик этапа stage_idx (0..NUM_STAGES-1).
    Для parallel>1: сначала занимаем ресурс, затем берём заявку — поток на следующий этап ровный,
    очередь следующего этапа не растёт при достаточной пропускной способности.
    """
    parallel = GEN_Stages[stage_idx]
    timer = GEN_Timers[stage_idx]
    is_last = (stage_idx == NUM_STAGES - 1)
    res = resources[stage_idx] if parallel > 1 else None

    while True:
        if res is not None:
            yield res.get(amount=parallel)
        request = yield in_queue.get()
        queue_counters[stage_idx] -= 1
        request.stage_starts[stage_idx] = env.now
        processing_counters[stage_idx] += 1
        yield env.timeout(timer)
        processing_counters[stage_idx] -= 1
        if res is not None:
            yield res.put(amount=parallel)
        request.stage_ends[stage_idx] = env.now

        if is_last:
            request.completion_time = env.now
            generation_times.append(request.completion_time - request.arrival_time)
        else:
            out_queue.put(request)
            queue_counters[stage_idx + 1] += 1


def progress_reporter(env, interval_sec=60):
    total_minutes = int(SIMULATION_TIME // 60)
    last_pct = -1
    while True:
        yield env.timeout(interval_sec)
        elapsed_min = int(env.now // 60)
        pct = int(100 * env.now / SIMULATION_TIME) if SIMULATION_TIME > 0 else 100
        if pct != last_pct and pct <= 100:
            print(f"  Progress: {pct}% ({elapsed_min} / {total_minutes} min)")
            last_pct = pct


def statistics_collector(env, queues, queue_counters, processing_counters, stats, minute_log):
    last_logged_minute = -1
    while True:
        current_time = env.now
        current_minute = int(current_time // 60)
        total_in_system = sum(queue_counters) + sum(processing_counters)

        # Пропускная способность этапа i: (GEN_Workers[i] / GEN_Stages[i]) / GEN_Timers[i]
        expected_total_wait = 0.0
        for i in range(NUM_STAGES):
            thr = (GEN_Workers[i] / GEN_Stages[i]) / GEN_Timers[i] if GEN_Timers[i] > 0 else 0
            if thr > 0:
                expected_total_wait += queue_counters[i] / thr
        expected_total_wait /= 60.0  # в минутах

        stats[current_minute]['main_queue'].append(total_in_system)
        for i in range(NUM_STAGES):
            stats[current_minute][f'stage{i}_queue'].append(queue_counters[i])
        stats[current_minute]['expected_wait_time'].append(expected_total_wait)
        stats[current_minute]['time'].append(current_time)

        if current_minute > last_logged_minute:
            if last_logged_minute >= 0 and stats[last_logged_minute]['main_queue']:
                prev = last_logged_minute
                rec = {'minute': prev, 'main_queue_avg': float(np.mean(stats[prev]['main_queue'])),
                       'expected_wait_time_avg': float(np.mean(stats[prev]['expected_wait_time'])),
                       'rejections': stats[prev]['rejections']}
                for i in range(NUM_STAGES):
                    rec[f'stage{i}_queue_avg'] = float(np.mean(stats[prev][f'stage{i}_queue']))
                minute_log.append(rec)
            last_logged_minute = current_minute

        yield env.timeout(1)


def run_simulation():
    """Запуск симуляции. SIMULATION_TIME задаётся из .env или из аргумента командной строки --hours."""
    sim_hours = SIMULATION_TIME / 3600.0

    env = simpy.Environment()
    queues = [simpy.Store(env, capacity=float('inf')) for _ in range(NUM_STAGES)]
    queue_counters = [0] * NUM_STAGES
    processing_counters = [0] * NUM_STAGES

    resources = []
    for i in range(NUM_STAGES):
        if GEN_Stages[i] > 1:
            res = simpy.Container(env, capacity=GEN_Workers[i], init=GEN_Workers[i])
            resources.append(res)
        else:
            resources.append(None)

    stats = defaultdict(lambda: {
        'main_queue': [], 'expected_wait_time': [], 'rejections': 0, 'time': [],
        **{f'stage{i}_queue': [] for i in range(NUM_STAGES)}
    })
    minute_log = []
    generation_times = []

    env.process(request_generator(env, queues, queue_counters, processing_counters, stats))

    # Для этапа с parallel>1 запускаем эффективное число воркеров (GEN_Workers//GEN_Stages),
    # каждый сначала занимает parallel единиц ресурса, затем берёт заявку — выход ровный, очередь 3-го этапа не растёт.
    # Для parallel=1 — GEN_Workers[i] независимых воркеров.
    for i in range(NUM_STAGES):
        out_q = queues[i + 1] if i < NUM_STAGES - 1 else None
        n_workers = (GEN_Workers[i] // GEN_Stages[i]) if GEN_Stages[i] > 1 else GEN_Workers[i]
        for _ in range(n_workers):
            env.process(stage_processor(
                env, i, queues[i], out_q, queue_counters, processing_counters,
                stats, resources, generation_times
            ))

    env.process(statistics_collector(env, queues, queue_counters, processing_counters, stats, minute_log))
    env.process(progress_reporter(env, interval_sec=60))

    print("Starting simulation...")
    print(f"Incoming rate: {INCOMING_RATE} requests/sec")
    for i in range(NUM_STAGES):
        thr = (GEN_Workers[i] / GEN_Stages[i]) / GEN_Timers[i] if GEN_Timers[i] > 0 else 0
        print(f"Stage {i+1} throughput: {thr:.2f} requests/sec (workers={GEN_Workers[i]}, parallel={GEN_Stages[i]}, time={GEN_Timers[i]}s)")
    print(f"Simulation time: {sim_hours} hours")
    env.run(until=SIMULATION_TIME)
    print("Simulation completed")

    last_minute = int(SIMULATION_TIME // 60) - 1
    if last_minute >= 0 and stats[last_minute]['main_queue']:
        rec = {'minute': last_minute, 'main_queue_avg': float(np.mean(stats[last_minute]['main_queue'])),
               'expected_wait_time_avg': float(np.mean(stats[last_minute]['expected_wait_time'])),
               'rejections': stats[last_minute]['rejections']}
        for i in range(NUM_STAGES):
            rec[f'stage{i}_queue_avg'] = float(np.mean(stats[last_minute][f'stage{i}_queue']))
        minute_log.append(rec)

    return stats, minute_log, generation_times


def generate_filename():
    parts = [str(INCOMING_RATE)] + [str(w) for w in GEN_Workers]
    return "_".join(parts)


def save_simulation_log(minute_log, filename):
    log_data = {
        'simulation_parameters': {
            'incoming_rate': INCOMING_RATE,
            'queue_limit': QUEUE_LIMIT,
            'simulation_time_seconds': SIMULATION_TIME,
            'simulation_time_hours': SIMULATION_TIME / 3600.0,
            'num_stages': NUM_STAGES,
            'GEN_Stages': GEN_Stages,
            'GEN_Timers': GEN_Timers,
            'GEN_Workers': GEN_Workers,
        },
        'minute_reports': minute_log
    }
    json_filename = f"{filename}.json"
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    print(f"Simulation log saved to file '{json_filename}'")
    return json_filename


def calculate_figure_size(num_subplots, height_per_subplot=None):
    if height_per_subplot is None:
        height_per_subplot = FIGURE_HEIGHT_PER_SUBPLOT
    base_height = 1.5
    total_height = base_height + (num_subplots * height_per_subplot)
    return (FIGURE_WIDTH, total_height)


def plot_statistics(stats, filename, generation_times=None):
    if generation_times is None:
        generation_times = []
    plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False

    NUM_SUBPLOTS = 5
    minutes = sorted([m for m in stats.keys() if m < SIMULATION_TIME // 60])
    avg_main_queue = []
    avg_expected_wait_times = []
    total_rejections = []
    minute_labels = []
    avg_stage_queues = [[] for _ in range(NUM_STAGES)]
    max_stage_queues = [[] for _ in range(NUM_STAGES)]

    for minute in minutes:
        if stats[minute]['main_queue']:
            avg_main_queue.append(np.mean(stats[minute]['main_queue']))
        else:
            avg_main_queue.append(0)
        for i in range(NUM_STAGES):
            key = f'stage{i}_queue'
            if stats[minute][key]:
                avg_stage_queues[i].append(np.mean(stats[minute][key]))
                max_stage_queues[i].append(np.max(stats[minute][key]))
            else:
                avg_stage_queues[i].append(0)
                max_stage_queues[i].append(0)
        if stats[minute]['expected_wait_time']:
            avg_expected_wait_times.append(np.mean(stats[minute]['expected_wait_time']))
        else:
            avg_expected_wait_times.append(0)
        total_rejections.append(stats[minute]['rejections'])
        minute_labels.append(minute)

    fig_width, fig_height = calculate_figure_size(NUM_SUBPLOTS)
    fig, axes = plt.subplots(NUM_SUBPLOTS, 1, figsize=(fig_width, fig_height))
    fig.suptitle('Queue system statistics (per minute)', fontsize=SUBTITLE_FONTSIZE, fontweight='bold')

    params_lines = [f"Incoming: {INCOMING_RATE} req/s", f"Queue limit: {QUEUE_LIMIT}", f"Simulation: {SIMULATION_TIME/3600:.2f} h"]
    for i in range(NUM_STAGES):
        params_lines.append(f"Stage {i+1}: workers={GEN_Workers[i]}, parallel={GEN_Stages[i]}, time={GEN_Timers[i]}s")
    params_text = "\n".join(params_lines)
    axes[0].text(0.98, 0.98, params_text, transform=axes[0].transAxes, fontsize=PARAMS_TEXT_FONTSIZE,
                 verticalalignment='top', horizontalalignment='right',
                 bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8), family='monospace')

    axes[0].plot(minute_labels, avg_main_queue, 'b-', linewidth=2, label='Main queue')
    axes[0].axhline(y=QUEUE_LIMIT, color='r', linestyle='--', linewidth=1, label=f'Limit ({QUEUE_LIMIT})')
    axes[0].set_xlabel('Minute', fontsize=AXIS_LABEL_FONTSIZE)
    axes[0].set_ylabel('Requests', fontsize=AXIS_LABEL_FONTSIZE)
    axes[0].set_title('Main queue (total in system)', fontsize=TITLE_FONTSIZE)
    axes[0].legend(fontsize=LEGEND_FONTSIZE)
    axes[0].grid(True, alpha=0.3)
    axes[0].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)

    colors = plt.cm.tab10(np.linspace(0, 1, max(NUM_STAGES, 1)))
    for i in range(NUM_STAGES):
        axes[1].plot(minute_labels, avg_stage_queues[i], color=colors[i % 10], linewidth=2, label=f'Stage {i+1}')
    axes[1].set_xlabel('Minute', fontsize=AXIS_LABEL_FONTSIZE)
    axes[1].set_ylabel('Requests', fontsize=AXIS_LABEL_FONTSIZE)
    axes[1].set_title('Internal queues per stage', fontsize=TITLE_FONTSIZE)
    axes[1].legend(fontsize=LEGEND_FONTSIZE)
    axes[1].grid(True, alpha=0.3)
    axes[1].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)

    axes[2].plot(minute_labels, avg_expected_wait_times, 'r-', linewidth=2)
    axes[2].set_xlabel('Minute', fontsize=AXIS_LABEL_FONTSIZE)
    axes[2].set_ylabel('Expected wait (min)', fontsize=AXIS_LABEL_FONTSIZE)
    axes[2].set_title('Expected wait time (for request arriving at this time)', fontsize=TITLE_FONTSIZE)
    axes[2].grid(True, alpha=0.3)
    axes[2].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)

    for i in range(NUM_STAGES):
        axes[3].plot(minute_labels, max_stage_queues[i], color=colors[i % 10], linewidth=2, label=f'Stage {i+1} max')
    axes[3].set_xlabel('Minute', fontsize=AXIS_LABEL_FONTSIZE)
    axes[3].set_ylabel('Max requests', fontsize=AXIS_LABEL_FONTSIZE)
    axes[3].set_title('Max queue per stage', fontsize=TITLE_FONTSIZE)
    axes[3].legend(fontsize=LEGEND_FONTSIZE)
    axes[3].grid(True, alpha=0.3)
    axes[3].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)

    axes[4].bar(minute_labels, total_rejections, color='red', alpha=0.7)
    axes[4].set_xlabel('Minute', fontsize=AXIS_LABEL_FONTSIZE)
    axes[4].set_ylabel('Rejections', fontsize=AXIS_LABEL_FONTSIZE)
    axes[4].set_title('Rejections (main queue limit exceeded)', fontsize=TITLE_FONTSIZE)
    axes[4].grid(True, alpha=0.3, axis='y')
    axes[4].tick_params(labelsize=AXIS_LABEL_FONTSIZE - 1)

    plt.tight_layout(rect=[0, BOTTOM_MARGIN, 1, TOP_MARGIN], h_pad=H_PAD)
    png_filename = f"{filename}.png"
    plt.savefig(png_filename, dpi=DPI, bbox_inches='tight', pad_inches=0.2)
    print(f"\nGraphs saved to file '{png_filename}'")
    plt.show()

    print("\n=== Overall Statistics ===")
    print(f"Total rejections: {sum(total_rejections)}")
    print(f"\nMain queue: avg={np.mean(avg_main_queue):.2f}, max={max(avg_main_queue) if avg_main_queue else 0:.2f}")
    for i in range(NUM_STAGES):
        print(f"Stage {i+1} queue: avg={np.mean(avg_stage_queues[i]):.2f}, max_avg={max(avg_stage_queues[i]) if avg_stage_queues[i] else 0:.2f}, max_peak={max(max_stage_queues[i]) if max_stage_queues[i] else 0:.2f}")
    print(f"\nExpected wait time: avg={np.mean(avg_expected_wait_times):.2f} min, max={max(avg_expected_wait_times) if avg_expected_wait_times else 0:.2f} min")
    print(f"\nGeneration time (arrival to completion):")
    if generation_times:
        avg_gen_sec = np.mean(generation_times)
        print(f"  Average (actual): {avg_gen_sec/60:.2f} min ({avg_gen_sec:.2f} sec)")
    theoretical_processing_sec = sum(GEN_Timers)
    avg_expected_wait_sec = np.mean(avg_expected_wait_times) * 60.0 if avg_expected_wait_times else 0.0
    expected_generation_sec = avg_expected_wait_sec + theoretical_processing_sec
    print(f"  Expected (wait + processing): {expected_generation_sec/60:.2f} min ({expected_generation_sec:.2f} sec)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Queue simulation with configurable stages')
    parser.add_argument('-t', '--hours', type=float, default=None,
                        help='Simulation time in hours (overrides .env SIMULATION_TIME_HOURS for this run)')
    args = parser.parse_args()
    if args.hours is not None:
        SIMULATION_TIME = int(args.hours * 3600)
        SIMULATION_TIME_HOURS = args.hours
        print(f"Using simulation time from command line: {args.hours} hours")

    random.seed(42)
    np.random.seed(42)

    filename = generate_filename()
    print(f"Results filename: {filename}")

    stats, minute_log, generation_times = run_simulation()
    save_simulation_log(minute_log, filename)
    plot_statistics(stats, filename, generation_times)
