import sys
import json
import threading
import subprocess
import platform
import os
import urllib.request
import zipfile
import tarfile
import time
import random
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Tuple, Callable
import numpy as np
import traceback

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QTabWidget, QLabel, QPushButton, 
                            QTextEdit, QGroupBox, QGridLayout, QSpinBox,
                            QDoubleSpinBox, QCheckBox, QMessageBox, QSplitter,
                            QFrame, QScrollArea, QProgressBar, QDialog)
from PyQt5.QtCore import QTimer, Qt, pyqtSignal, QObject, QThread
from PyQt5.QtGui import QFont, QColor, QPalette
import pyqtgraph as pg

# Настройка стилей для темной темы
DARK_STYLE = """
QMainWindow {
    background-color: #2b2b2b;
}
QWidget {
    background-color: #2b2b2b;
    color: #ffffff;
    font-family: 'Segoe UI', Arial, sans-serif;
}
QGroupBox {
    border: 2px solid #555;
    border-radius: 5px;
    margin-top: 1ex;
    font-weight: bold;
    color: #ffffff;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 5px 0 5px;
}
QPushButton {
    background-color: #0d7377;
    border: none;
    border-radius: 3px;
    padding: 8px;
    color: white;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #14a085;
}
QPushButton:pressed {
    background-color: #0a5e62;
}
QPushButton:disabled {
    background-color: #444;
    color: #777;
}
QTextEdit {
    background-color: #1e1e1e;
    border: 1px solid #555;
    border-radius: 3px;
    font-family: 'Consolas', 'Courier New', monospace;
}
QTabWidget::pane {
    border: 2px solid #555;
    background-color: #2b2b2b;
}
QTabBar::tab {
    background-color: #3c3c3c;
    padding: 8px;
    margin-right: 2px;
}
QTabBar::tab:selected {
    background-color: #0d7377;
}
QTabBar::tab:hover {
    background-color: #4a4a4a;
}
QSpinBox, QDoubleSpinBox {
    background-color: #3c3c3c;
    border: 1px solid #555;
    border-radius: 3px;
    padding: 3px;
}
QProgressBar {
    border: 1px solid #555;
    border-radius: 3px;
    text-align: center;
}
QProgressBar::chunk {
    background-color: #0d7377;
    border-radius: 3px;
}
"""

class DownloadProgress:
    """Класс для отслеживания прогресса скачивания"""
    def __init__(self, progress_callback: Callable[[int, str], None]):
        self.progress_callback = progress_callback
        self.last_percent = 0
        
    def report(self, block_num, block_size, total_size):
        """Callback для urllib.request.urlretrieve"""
        if total_size > 0:
            downloaded = block_num * block_size
            percent = min(int(100 * downloaded / total_size), 99)  # Не показываем 100% до завершения
            if percent > self.last_percent:
                self.last_percent = percent
                self.progress_callback(percent, f"Скачивание... {percent}%")

class DataStreamSignals(QObject):
    """Сигналы для потока данных"""
    new_data = pyqtSignal(dict)
    alert = pyqtSignal(str, str)
    stats_updated = pyqtSignal(dict)
    kafka_status = pyqtSignal(str)

class KafkaManager(QThread):
    """Управление Kafka в отдельном потоке"""
    status_update = pyqtSignal(str)
    download_progress = pyqtSignal(int, str)  # (процент, статус)
    
    def __init__(self):
        super().__init__()
        self.kafka_processes = []
        self.running = False
        
    def run(self):
        """Запуск Kafka в фоновом режиме"""
        try:
            system = platform.system()
            
            # Проверка наличия Java
            try:
                java_version = subprocess.run(['java', '-version'], 
                                            capture_output=True, text=True, timeout=10)
                self.status_update.emit("✅ Java найдена")
            except (FileNotFoundError, subprocess.TimeoutExpired) as e:
                self.status_update.emit("❌ Java не установлена! Установите Java 8+")
                return
            
            # Путь к Kafka
            kafka_dir = os.path.join(os.path.expanduser("~"), "kafka_2.13-3.5.1")
            
            if not os.path.exists(kafka_dir):
                self.status_update.emit("⚠️ Kafka не найдена, скачиваю...")
                self.download_kafka_with_progress()
            
            if os.path.exists(kafka_dir):
                os.chdir(kafka_dir)
                
                # Запуск Zookeeper
                if system == "Windows":
                    zk_cmd = ["bin\\windows\\zookeeper-server-start.bat", "config\\zookeeper.properties"]
                    kafka_cmd = ["bin\\windows\\kafka-server-start.bat", "config\\server.properties"]
                    topic_cmd = ["bin\\windows\\kafka-topics.bat", "--create", 
                               "--topic", "iot-rooms", "--bootstrap-server", "localhost:9092",
                               "--partitions", "3", "--replication-factor", "1"]
                else:  # Linux/Mac
                    zk_cmd = ["./bin/zookeeper-server-start.sh", "config/zookeeper.properties"]
                    kafka_cmd = ["./bin/kafka-server-start.sh", "config/server.properties"]
                    topic_cmd = ["./bin/kafka-topics.sh", "--create",
                               "--topic", "iot-rooms", "--bootstrap-server", "localhost:9092",
                               "--partitions", "3", "--replication-factor", "1"]
                
                self.status_update.emit("🚀 Запуск Zookeeper...")
                self.download_progress.emit(10, "Запуск Zookeeper...")
                try:
                    zk_process = subprocess.Popen(zk_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.kafka_processes.append(zk_process)
                    time.sleep(5)  # Ждем запуск Zookeeper
                except Exception as e:
                    self.status_update.emit(f"❌ Ошибка запуска Zookeeper: {str(e)}")
                    return
                
                self.status_update.emit("🚀 Запуск Kafka broker...")
                self.download_progress.emit(30, "Запуск Kafka broker...")
                try:
                    kafka_process = subprocess.Popen(kafka_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.kafka_processes.append(kafka_process)
                    time.sleep(5)
                except Exception as e:
                    self.status_update.emit(f"❌ Ошибка запуска Kafka: {str(e)}")
                    return
                
                # Создание топика
                self.status_update.emit("📝 Создание топика iot-rooms...")
                self.download_progress.emit(50, "Создание топика...")
                try:
                    subprocess.run(topic_cmd, capture_output=True, timeout=10)
                except:
                    pass  # Топик может уже существовать
                
                self.download_progress.emit(100, "✅ Kafka успешно запущена")
                self.status_update.emit("✅ Kafka успешно запущена")
            else:
                self.status_update.emit("❌ Не удалось найти Kafka после скачивания")
                
        except Exception as e:
            self.status_update.emit(f"❌ Ошибка запуска Kafka: {str(e)}")
    
    def download_kafka_with_progress(self):
        """Скачивание Kafka с отслеживанием прогресса"""
        try:
            system = platform.system()
            
            # Список зеркал для скачивания Kafka
            kafka_urls = [
                "https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz",
                "https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz",
                "https://mirror.linux-ia64.org/apache/kafka/3.5.1/kafka_2.13-3.5.1.tgz",
                "https://mirrors.gigenet.com/apache/kafka/3.5.1/kafka_2.13-3.5.1.tgz"
            ]
            
            filename = "kafka.tgz"
            downloaded = False
            
            for i, kafka_url in enumerate(kafka_urls):
                try:
                    self.status_update.emit(f"📥 Попытка {i+1}/{len(kafka_urls)}: {kafka_url}")
                    self.download_progress.emit(5 + i*10, f"Попытка скачивания {i+1}...")
                    
                    # Создаем объект для отслеживания прогресса
                    progress = DownloadProgress(lambda p, s: self.download_progress.emit(20 + p//2, s))
                    
                    # Скачивание с таймаутом и отслеживанием прогресса
                    req = urllib.request.Request(kafka_url, headers={'User-Agent': 'Mozilla/5.0'})
                    
                    # Используем urlretrieve с callback для прогресса
                    urllib.request.urlretrieve(
                        kafka_url, 
                        filename, 
                        reporthook=progress.report
                    )
                    
                    # Проверяем размер файла
                    if os.path.exists(filename):
                        file_size = os.path.getsize(filename)
                        if file_size < 1000000:  # Меньше 1MB - вероятно ошибка
                            raise Exception("Файл слишком мал, вероятно ошибка скачивания")
                    
                    downloaded = True
                    self.download_progress.emit(70, "✅ Скачивание завершено")
                    self.status_update.emit(f"✅ Успешно скачано с {kafka_url}")
                    break
                    
                except Exception as e:
                    self.status_update.emit(f"❌ Ошибка скачивания: {str(e)}")
                    if os.path.exists(filename):
                        try:
                            os.remove(filename)
                        except:
                            pass
                    continue
            
            if not downloaded:
                self.download_progress.emit(0, "❌ Ошибка скачивания")
                self.status_update.emit("❌ Не удалось скачать Kafka ни с одного зеркала")
                self.status_update.emit("💡 Используйте ручную установку из инструкции")
                return
            
            # Распаковка
            self.status_update.emit("📦 Распаковка Kafka...")
            self.download_progress.emit(80, "Распаковка...")
            
            try:
                import tarfile
                with tarfile.open(filename, 'r:gz') as tar:
                    # Получаем список файлов для отслеживания прогресса
                    members = tar.getmembers()
                    total_members = len(members)
                    
                    for j, member in enumerate(members):
                        tar.extract(member, os.path.expanduser("~"))
                        if j % 10 == 0:  # Обновляем прогресс каждые 10 файлов
                            percent = 80 + int(20 * (j / total_members))
                            self.download_progress.emit(percent, f"Распаковка... {percent-80}%")
                
                # Переименовываем папку если нужно
                extracted_dir = os.path.join(os.path.expanduser("~"), "kafka_2.13-3.5.1")
                target_dir = os.path.join(os.path.expanduser("~"), "kafka_2.13-3.5.1")
                if os.path.exists(extracted_dir) and not os.path.exists(target_dir):
                    os.rename(extracted_dir, target_dir)
                
                # Удаление архива
                if os.path.exists(filename):
                    os.remove(filename)
                
                self.download_progress.emit(100, "✅ Kafka установлена")
                self.status_update.emit("✅ Kafka скачана и распакована")
                
            except Exception as e:
                self.status_update.emit(f"❌ Ошибка распаковки: {str(e)}")
                self.download_progress.emit(0, f"❌ Ошибка: {str(e)}")
            
        except Exception as e:
            self.status_update.emit(f"❌ Ошибка скачивания Kafka: {str(e)}")
            self.download_progress.emit(0, f"❌ Ошибка: {str(e)}")
    
    def stop(self):
        """Остановка Kafka"""
        self.running = False
        for process in self.kafka_processes:
            try:
                if process.poll() is None:  # Процесс еще работает
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
            except Exception as e:
                print(f"Ошибка при остановке процесса: {e}")
        self.kafka_processes = []

class DataGenerator:
    """Генератор данных датчиков с отправкой в Kafka"""
    def __init__(self, rooms=None):
        self.rooms = rooms or ['Kitchen', 'Living Room', 'Bedroom', 'Bathroom', 'Office']
        self.running = False
        self.producer = None
        self.thread = None
        self.temp_threshold = 30
        self.signals = DataStreamSignals()
        self.lock = threading.Lock()
        self.init_kafka_producer()
        
    def init_kafka_producer(self):
        """Инициализация Kafka producer"""
        try:
            # Проверяем доступность confluent_kafka
            from confluent_kafka import Producer
            conf = {
                'bootstrap.servers': 'localhost:9092',
                'client.id': 'iot-generator',
                'acks': 'all',
                'retries': 3
            }
            self.producer = Producer(conf)
            self.signals.kafka_status.emit("✅ Kafka producer инициализирован")
        except ImportError:
            self.signals.kafka_status.emit("⚠️ confluent-kafka не установлен. Установите: pip install confluent-kafka")
            self.producer = None
        except Exception as e:
            self.signals.kafka_status.emit(f"❌ Ошибка Kafka: {str(e)}")
            self.producer = None
            
    def generate_sensor_data(self):
        """Генерация случайных данных и отправка в Kafka"""
        try:
            while self.running:
                with self.lock:
                    if not self.running:
                        break
                
                for room in self.rooms:
                    if not self.running:
                        break
                        
                    # Генерация реалистичных данных
                    temperature = random.gauss(23, 3)  # Средняя 23°C, отклонение 3
                    humidity = random.gauss(45, 10)    # Средняя 45%, отклонение 10
                    motion = random.random() < 0.3     # 30% вероятность движения
                    
                    # Иногда генерируем аномалии
                    if random.random() < 0.1:  # 10% вероятность аномалии
                        temperature = random.uniform(31, 38)
                        if random.random() < 0.5:
                            humidity = random.uniform(10, 20) or random.uniform(70, 90)
                        
                    data = {
                        'room': room,
                        'temperature': round(temperature, 1),
                        'humidity': round(humidity, 1),
                        'motion': motion,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Отправка в Kafka
                    if self.producer and self.running:
                        try:
                            self.producer.produce(
                                'iot-rooms',
                                key=room.encode('utf-8'),
                                value=json.dumps(data).encode('utf-8')
                            )
                            self.producer.poll(0)
                        except Exception as e:
                            self.signals.kafka_status.emit(f"❌ Ошибка отправки: {str(e)}")
                    
                    # Отправка сигнала для GUI
                    if self.running:
                        try:
                            self.signals.new_data.emit(data)
                        except RuntimeError:
                            break  # GUI уже уничтожен
                    
                    # Проверка на перегрев (дополнительная проверка)
                    if temperature > self.temp_threshold  and self.running:
                        try:
                            self.signals.alert.emit(
                                room,
                                f"⚠️ ПЕРЕГРЕВ! Температура {temperature:.1f}°C превышает критическое значение!"
                            )
                        except:
                            pass
                    
                    # Проверка на движение
                    if motion and self.running:
                        try:
                            self.signals.alert.emit(
                                room,
                                f"👤 Обнаружено движение в {room}"
                            )
                        except:
                            pass
                    
                if self.running:
                    time.sleep(1)  # Отправка данных раз в секунду
        except Exception as e:
            print(f"Ошибка в генераторе данных: {e}")
            traceback.print_exc()
    
    def start(self):
        """Запуск генератора данных"""
        self.running = True
        self.thread = threading.Thread(target=self.generate_sensor_data)
        self.thread.daemon = False
        self.thread.start()
        
    def stop(self):
        print("Остановка генератора данных...")
        self.running = False

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

        if self.producer:
            try:
                self.producer.flush(2)
            except:
                pass

        print("Генератор данных остановлен")
    
    def set_threshold(self, value):
        self.temp_threshold = float(value)

class FlinkProcessor(QThread):
    """Обработка данных через Apache Flink"""
    data_received = pyqtSignal(dict)
    alert = pyqtSignal(str, str)
    
    def __init__(self):
        super().__init__()
        self.running = False
        self.consumer = None
        self.lock = threading.Lock()
        self.init_kafka_consumer()
        
    def init_kafka_consumer(self):
        """Инициализация Kafka consumer"""
        try:
            from confluent_kafka import Consumer
            conf = {
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'flink-processor',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'session.timeout.ms': 6000,
                'max.poll.interval.ms': 300000
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe(['iot-rooms'])
        except ImportError:
            print("⚠️ confluent-kafka не установлен")
        except Exception as e:
            print(f"Ошибка consumer: {str(e)}")
            
    def run(self):
        """Основной цикл обработки"""
        self.running = True
        
        # Хранилище для данных за последние 5 минут
        window_data = defaultdict(list)
        
        try:
            while self.running and not self.isInterruptionRequested():
                try:
                    with self.lock:
                        if not self.running:
                            break
                    
                    if self.consumer:
                        msg = self.consumer.poll(0.1)  # Уменьшаем время ожидания
                        if msg is None:
                            continue
                        if msg.error():
                            continue
                            
                        # Парсинг JSON данных
                        data = json.loads(msg.value().decode('utf-8'))
                        
                        # Сохраняем в окно
                        window_data[data['room']].append(data)
                        
                        # Очистка старых данных (старше 5 минут)
                        current_time = datetime.fromisoformat(data['timestamp'])
                        window_data[data['room']] = [
                            d for d in window_data[data['room']]
                            if (current_time - datetime.fromisoformat(d['timestamp'])).total_seconds() <= 300
                        ]
                        
                        # Расчет средних за 5 минут
                        if len(window_data[data['room']]) >= 5:  # Минимум 5 измерений
                            avg_temp = sum(d['temperature'] for d in window_data[data['room']]) / len(window_data[data['room']])
                            avg_hum = sum(d['humidity'] for d in window_data[data['room']]) / len(window_data[data['room']])
                            
                            # Проверка комфортности
                            if avg_temp < 20 or avg_temp > 26 or avg_hum < 30 or avg_hum > 60:
                                comfort_msg = f"📊 Статистика за 5 мин в {data['room']}: "
                                comfort_msg += f"Ср. темп: {avg_temp:.1f}°C, Ср. влаж: {avg_hum:.1f}%"
                                try:
                                    if self.running:
                                        self.alert.emit(data['room'], comfort_msg)
                                except:
                                    pass
                        
                        # Отправка данных в GUI
                        try:
                            if self.running:
                                self.data_received.emit(data)
                        except:
                            pass
                        
                except Exception as e:
                    if not self.running:
                        break
                    print(f"Ошибка в цикле обработки: {e}")
                    QApplication.processEvents()
        except Exception as e:
            print(f"Критическая ошибка в FlinkProcessor: {e}")
            traceback.print_exc()
        finally:
            print("FlinkProcessor завершает работу")

            if self.consumer:
                try:
                    self.consumer.close()
                except Exception as e:
                    print(f"Ошибка при закрытии consumer: {e}")
    
    def stop(self):
        """Остановка обработчика"""
        print("Остановка FlinkProcessor...")
        self.running = False
        self.requestInterruption()
        
        # if self.consumer:
        #     try:
        #         self.consumer.close()
        #     except Exception as e:
        #         print(f"Ошибка при закрытии consumer: {e}")
        
        # Ждем завершения потока
        if self.isRunning():
            self.wait(3000)  # Ждем до 3 секунд
        print("FlinkProcessor остановлен")

class RoomData:
    """Класс для хранения данных комнаты"""
    def __init__(self, name):
        self.name = name
        self.temperatures = deque(maxlen=300)  # 5 минут при 1 измерении в секунду
        self.humidities = deque(maxlen=300)
        self.motions = deque(maxlen=600)  # 10 минут при 1 измерении в секунду
        self.timestamps = deque(maxlen=300)
        self.alerts = []
        self.lock = threading.Lock()
        
    def add_reading(self, temperature, humidity, motion, timestamp):
        with self.lock:
            self.temperatures.append(temperature)
            self.humidities.append(humidity)
            self.motions.append(motion)
            self.timestamps.append(timestamp)
        
    def get_avg_temperature_5min(self):
        with self.lock:
            if len(self.temperatures) > 0:
                return sum(self.temperatures) / len(self.temperatures)
            return 0
        
    def get_avg_humidity_5min(self):
        with self.lock:
            if len(self.humidities) > 0:
                return sum(self.humidities) / len(self.humidities)
            return 0
        
    def has_motion_10min(self):
        with self.lock:
            # Проверяем движение за последние 10 минут
            if len(self.motions) > 0:
                recent_motions = list(self.motions)[-600:]  # последние 600 записей
                return any(recent_motions)
            return False
        
    def get_temperature_history(self):
        with self.lock:
            return list(self.temperatures)
        
    def get_humidity_history(self):
        with self.lock:
            return list(self.humidities)

class RoomWidget(QWidget):
    """Виджет для отображения данных комнаты"""
    def __init__(self, room_name):
        super().__init__()
        self.room_name = room_name
        self.temp_threshold = 30  # значение по умолчанию
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout()
        
        # Графики температуры и влажности
        self.temp_plot = pg.PlotWidget(title=f"Температура - {self.room_name}")
        self.temp_plot.setLabel('left', 'Температура', units='°C')
        self.temp_plot.setLabel('bottom', 'Время', units='с')
        self.temp_plot.showGrid(x=True, y=True)
        self.temp_plot.setYRange(10, 40)
        self.temp_curve = self.temp_plot.plot(pen=pg.mkPen(color='r', width=2))
        
        # Добавление линии критической температуры
        self.critical_line = pg.InfiniteLine(pos=30, angle=0, pen=pg.mkPen(color='#ff0000', style=Qt.DashLine))
        self.temp_plot.addItem(self.critical_line)
        
        self.hum_plot = pg.PlotWidget(title=f"Влажность - {self.room_name}")
        self.hum_plot.setLabel('left', 'Влажность', units='%')
        self.hum_plot.setLabel('bottom', 'Время', units='с')
        self.hum_plot.showGrid(x=True, y=True)
        self.hum_plot.setYRange(0, 100)
        self.hum_curve = self.hum_plot.plot(pen=pg.mkPen(color='b', width=2))
        
        # Текущие показатели
        stats_group = QGroupBox("Текущие показатели")
        stats_layout = QGridLayout()
        
        self.temp_label = QLabel("Температура: -- °C")
        self.temp_label.setStyleSheet("font-size: 14pt;")
        stats_layout.addWidget(self.temp_label, 0, 0)
        
        self.hum_label = QLabel("Влажность: -- %")
        self.hum_label.setStyleSheet("font-size: 14pt;")
        stats_layout.addWidget(self.hum_label, 0, 1)
        
        self.motion_label = QLabel("Движение: --")
        self.motion_label.setStyleSheet("font-size: 14pt;")
        stats_layout.addWidget(self.motion_label, 1, 0)
        
        self.avg_temp_label = QLabel("Ср. темп. (5 мин): -- °C")
        stats_layout.addWidget(self.avg_temp_label, 1, 1)
        
        self.avg_hum_label = QLabel("Ср. влажн. (5 мин): -- %")
        stats_layout.addWidget(self.avg_hum_label, 2, 0)
        
        self.motion_history_label = QLabel("Движение за 10 мин: --")
        self.motion_history_label.setStyleSheet("font-size: 12pt;")
        stats_layout.addWidget(self.motion_history_label, 2, 1)
        
        stats_group.setLayout(stats_layout)
        
        layout.addWidget(self.temp_plot)
        layout.addWidget(self.hum_plot)
        layout.addWidget(stats_group)
        
        self.setLayout(layout)
        
    def update_data(self, stats):
        """Обновление данных на виджете"""
        try:
            # Обновление графиков
            if stats.get('temp_history'):
                self.temp_curve.setData(stats['temp_history'])
            if stats.get('hum_history'):
                self.hum_curve.setData(stats['hum_history'])
                
            # Обновление текстовых меток
            if 'current_temp' in stats:
                self.temp_label.setText(f"Температура: {stats['current_temp']:.1f} °C")
            if 'current_hum' in stats:
                self.hum_label.setText(f"Влажность: {stats['current_hum']:.1f} %")
            
            if 'current_motion' in stats:
                motion_text = "✅ Есть" if stats['current_motion'] else "❌ Нет"
                motion_color = "#00ff00" if stats['current_motion'] else "#ff0000"
                self.motion_label.setText(f"Движение: {motion_text}")
                self.motion_label.setStyleSheet(f"font-size: 14pt; color: {motion_color};")
            
            if 'avg_temp_5min' in stats:
                self.avg_temp_label.setText(f"Ср. темп. (5 мин): {stats['avg_temp_5min']:.1f} °C")
            if 'avg_hum_5min' in stats:
                self.avg_hum_label.setText(f"Ср. влажн. (5 мин): {stats['avg_hum_5min']:.1f} %")
            
            if 'has_motion_10min' in stats:
                motion_history_text = "✅ Было движение" if stats['has_motion_10min'] else "❌ Не было движения"
                motion_history_color = "#00ff00" if stats['has_motion_10min'] else "#ff0000"
                self.motion_history_label.setText(f"Движение за 10 мин: {motion_history_text}")
                self.motion_history_label.setStyleSheet(f"font-size: 12pt; color: {motion_history_color};")
            
            # Изменение цвета при превышении температуры
            if stats.get('current_temp', 0) > self.temp_threshold:
                self.temp_label.setStyleSheet("font-size: 14pt; color: #ff0000; font-weight: bold;")
            else:
                self.temp_label.setStyleSheet("font-size: 14pt;")
        except Exception as e:
            print(f"Ошибка при обновлении данных виджета: {e}")

    def update_thresholds(self, temp_threshold, comfort_temp_min, comfort_temp_max, comfort_hum_min, comfort_hum_max):
        """Обновление пороговых значений на графиках"""
        try:
            self.temp_threshold = temp_threshold
            # Обновляем линию критической температуры
            self.critical_line.setValue(temp_threshold)
            
            # Обновляем цветовую индикацию на основе новых порогов
            self.temp_plot.setYRange(comfort_temp_min - 5, comfort_temp_max + 10)
        except Exception as e:
            print(f"Ошибка при обновлении порогов: {e}")

class SetupWizard(QDialog):
    """Мастер установки зависимостей и Kafka"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Мастер установки")
        self.setGeometry(200, 200, 600, 550)
        self.setStyleSheet(DARK_STYLE)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout()
        
        # Заголовок
        title = QLabel("Установка необходимых компонентов")
        title.setStyleSheet("font-size: 16pt; font-weight: bold;")
        layout.addWidget(title)
        
        # Описание
        desc = QLabel("Этот мастер поможет установить Apache Kafka и зависимости")
        layout.addWidget(desc)
        
        # Информация о компонентах
        components_group = QGroupBox("Необходимые компоненты")
        components_layout = QVBoxLayout()
        
        self.java_status = QLabel("Java: ❓ Не проверено")
        components_layout.addWidget(self.java_status)
        
        self.kafka_status = QLabel("Kafka: ❓ Не проверено")
        components_layout.addWidget(self.kafka_status)
        
        self.python_status = QLabel("Python пакеты: ❓ Не проверено")
        components_layout.addWidget(self.python_status)
        
        components_group.setLayout(components_layout)
        layout.addWidget(components_group)
        
        # Прогресс бар
        self.progress = QProgressBar()
        self.progress.setValue(0)
        layout.addWidget(self.progress)
        
        # Статус прогресса
        self.progress_status = QLabel("Готов к установке")
        self.progress_status.setStyleSheet("font-size: 10pt; color: #888888;")
        layout.addWidget(self.progress_status)
        
        # Лог установки
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumHeight(150)
        layout.addWidget(self.log)
        
        # Кнопки
        btn_layout = QHBoxLayout()
        
        self.check_btn = QPushButton("🔍 Проверить компоненты")
        self.check_btn.clicked.connect(self.check_components)
        btn_layout.addWidget(self.check_btn)
        
        self.install_btn = QPushButton("📦 Установить всё автоматически")
        self.install_btn.clicked.connect(self.auto_install)
        btn_layout.addWidget(self.install_btn)
        
        self.manual_btn = QPushButton("📖 Показать инструкцию")
        self.manual_btn.clicked.connect(self.show_manual)
        btn_layout.addWidget(self.manual_btn)
        
        self.close_btn = QPushButton("❌ Закрыть")
        self.close_btn.clicked.connect(self.close)
        btn_layout.addWidget(self.close_btn)
        
        layout.addLayout(btn_layout)
        self.setLayout(layout)
        
    def log_message(self, msg):
        self.log.append(msg)
        # Автопрокрутка
        scrollbar = self.log.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
    def update_progress(self, value, status):
        """Обновление прогресс-бара и статуса"""
        self.progress.setValue(value)
        self.progress_status.setText(status)
        QApplication.processEvents()  # Обновляем интерфейс
        
    def check_components(self):
        """Проверка установленных компонентов"""
        self.log_message("🔍 Проверка компонентов...")
        
        # Проверка Java
        try:
            java_version = subprocess.run(['java', '-version'], 
                                        capture_output=True, text=True, timeout=10)
            self.java_status.setText("Java: ✅ Установлена")
            self.java_status.setStyleSheet("color: #00ff00;")
            self.log_message("✅ Java найдена")
        except:
            self.java_status.setText("Java: ❌ Не установлена")
            self.java_status.setStyleSheet("color: #ff0000;")
            self.log_message("❌ Java не найдена")
        
        # Проверка Kafka
        kafka_dir = os.path.join(os.path.expanduser("~"), "kafka_2.13-3.5.1")
        if os.path.exists(kafka_dir):
            self.kafka_status.setText("Kafka: ✅ Установлена")
            self.kafka_status.setStyleSheet("color: #00ff00;")
            self.log_message("✅ Kafka найдена")
        else:
            self.kafka_status.setText("Kafka: ❌ Не установлена")
            self.kafka_status.setStyleSheet("color: #ff0000;")
            self.log_message("❌ Kafka не найдена")
        
        # Проверка Python пакетов
        packages_ok = True
        try:
            import confluent_kafka
            self.log_message("✅ confluent-kafka установлен")
        except:
            packages_ok = False
            self.log_message("❌ confluent-kafka не установлен")
            
        if packages_ok:
            self.python_status.setText("Python пакеты: ✅ Установлены")
            self.python_status.setStyleSheet("color: #00ff00;")
        else:
            self.python_status.setText("Python пакеты: ❌ Не все установлены")
            self.python_status.setStyleSheet("color: #ff0000;")
        
    def auto_install(self):
        """Автоматическая установка"""
        self.install_btn.setEnabled(False)
        self.check_btn.setEnabled(False)
        self.progress.setValue(0)
        
        try:
            # Установка Python пакетов
            self.log_message("📦 Установка Python пакетов...")
            self.update_progress(5, "Установка Python пакетов...")
            
            packages = [
                "confluent-kafka",
                "pyqt5",
                "pyqtgraph",
                "numpy"
            ]
            
            for i, package in enumerate(packages):
                self.log_message(f"  Установка {package}...")
                self.update_progress(5 + i*10, f"Установка {package}...")
                
                try:
                    result = subprocess.run([sys.executable, "-m", "pip", "install", package, "--quiet"], 
                                 capture_output=True, text=True, timeout=120)
                    if result.returncode == 0:
                        self.log_message(f"  ✅ {package} установлен")
                    else:
                        self.log_message(f"  ❌ Ошибка: {result.stderr[:200]}")
                except Exception as e:
                    self.log_message(f"  ❌ Ошибка установки {package}: {str(e)}")
                
                self.update_progress(5 + (i+1)*10, f"Установлен {package}")
            
            # Скачивание Kafka
            self.log_message("📥 Скачивание Apache Kafka...")
            self.update_progress(50, "Подготовка к скачиванию Kafka...")
            
            # Создаем экземпляр KafkaManager для скачивания
            self.kafka_manager = KafkaManager()
            self.kafka_manager.status_update.connect(self.log_message)
            self.kafka_manager.download_progress.connect(self.update_progress)
            
            # Запускаем скачивание в том же потоке
            self.kafka_manager.download_kafka_with_progress()
            
            self.update_progress(100, "✅ Установка завершена!")
            self.log_message("✅ Установка завершена!")
            
            QMessageBox.information(self, "Успех", "Все компоненты успешно установлены!\n\nТеперь вы можете запустить Kafka кнопкой 'Запустить Kafka' в главном окне.")
            
        except Exception as e:
            self.log_message(f"❌ Ошибка: {str(e)}")
            self.update_progress(0, f"❌ Ошибка: {str(e)[:50]}")
            QMessageBox.warning(self, "Ошибка", f"Ошибка установки: {str(e)}\n\nИспользуйте ручную установку")
            
        self.install_btn.setEnabled(True)
        self.check_btn.setEnabled(True)
        self.check_components()
        
    def show_manual(self):
        """Показать инструкцию по ручной установке"""
        system = platform.system()
        
        if system == "Windows":
            manual = """РУЧНАЯ УСТАНОВКА ДЛЯ WINDOWS:

1. Установка Java:
   - Скачайте Java с https://www.java.com/download/
   - Установите и проверьте: java -version

2. Установка Python пакетов:
   Откройте командную строку (cmd) от имени администратора:
   
   pip install confluent-kafka pyqt5 pyqtgraph numpy

3. Установка Apache Kafka:
   - Скачайте Kafka с https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
   - Распакуйте архив в C:\\Users\\ВашеИмя\\kafka_2.13-3.5.1

4. Запуск Kafka:
   Откройте ДВА окна командной строки:
   
   Окно 1 (Zookeeper):
   cd %USERPROFILE%\\kafka_2.13-3.5.1
   bin\\windows\\zookeeper-server-start.bat config\\zookeeper.properties
   
   Окно 2 (Kafka):
   cd %USERPROFILE%\\kafka_2.13-3.5.1
   bin\\windows\\kafka-server-start.bat config\\server.properties
   
   Окно 3 (создание топика):
   cd %USERPROFILE%\\kafka_2.13-3.5.1
   bin\\windows\\kafka-topics.bat --create --topic iot-rooms --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
"""
        else:  # Linux
            manual = """РУЧНАЯ УСТАНОВКА ДЛЯ LINUX:

1. Установка Java:
   sudo apt update
   sudo apt install default-jre default-jdk
   java -version

2. Установка Python пакетов:
   pip3 install confluent-kafka pyqt5 pyqtgraph numpy

3. Установка Apache Kafka:
   wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
   tar -xzf kafka_2.13-3.5.1.tgz
   mv kafka_2.13-3.5.1 ~/kafka_2.13-3.5.1

4. Запуск Kafka (нужно 3 терминала):
   
   Терминал 1 - Zookeeper:
   cd ~/kafka_2.13-3.5.1
   ./bin/zookeeper-server-start.sh config/zookeeper.properties
   
   Терминал 2 - Kafka:
   cd ~/kafka_2.13-3.5.1
   ./bin/kafka-server-start.sh config/server.properties
   
   Терминал 3 - создание топика:
   cd ~/kafka_2.13-3.5.1
   ./bin/kafka-topics.sh --create --topic iot-rooms --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
"""
        
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Инструкция по ручной установке")
        msg_box.setText(manual)
        msg_box.setTextInteractionFlags(Qt.TextSelectableByMouse)
        msg_box.setMinimumWidth(600)
        msg_box.exec_()

class MainWindow(QMainWindow):
    """Главное окно приложения"""
    def __init__(self):
        super().__init__()
        self.data_generator = None
        self.flink_processor = None
        self.kafka_manager = None
        self.rooms_data = {}
        self.room_widgets = {}
        self.alerts = []
        self.is_stopping = False
        
        self.init_ui()
        QTimer.singleShot(100, self.check_setup)
        
    def check_setup(self):
        """Проверка наличия всех компонентов"""
        try:
            # Проверка Kafka
            import confluent_kafka
            self.log_message("✅ confluent-kafka найден")
            
        except ImportError as e:
            reply = QMessageBox.question(
                self, "Компоненты не найдены",
                "Библиотека confluent-kafka не установлена.\n\nОткрыть мастер установки?",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                wizard = SetupWizard(self)
                wizard.exec_()
        
    def init_ui(self):
        self.setWindowTitle("IoT Stream Processing - Умный дом (Apache Kafka + Flink)")
        self.setGeometry(100, 100, 1400, 800)
        
        # Центральный виджет
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Главный layout
        main_layout = QHBoxLayout(central_widget)
        
        # Левая панель - управление
        left_panel = QWidget()
        left_panel.setMaximumWidth(350)
        left_layout = QVBoxLayout(left_panel)
        
        # Группа управления Kafka
        kafka_group = QGroupBox("Управление Kafka")
        kafka_layout = QVBoxLayout()
        
        self.start_kafka_btn = QPushButton("🚀 Запустить Kafka")
        self.start_kafka_btn.clicked.connect(self.start_kafka)
        kafka_layout.addWidget(self.start_kafka_btn)
        
        self.stop_kafka_btn = QPushButton("⏹ Остановить Kafka")
        self.stop_kafka_btn.clicked.connect(self.stop_kafka)
        self.stop_kafka_btn.setEnabled(False)
        kafka_layout.addWidget(self.stop_kafka_btn)
        
        self.kafka_status = QLabel("Статус: ⚫ Остановлен")
        self.kafka_status.setStyleSheet("color: #ff0000;")
        kafka_layout.addWidget(self.kafka_status)
        
        kafka_group.setLayout(kafka_layout)
        left_layout.addWidget(kafka_group)
        
        # Группа генерации данных
        gen_group = QGroupBox("Генерация данных")
        gen_layout = QVBoxLayout()
        
        self.start_gen_btn = QPushButton("▶ Запустить генерацию")
        self.start_gen_btn.clicked.connect(self.start_generation)
        self.start_gen_btn.setEnabled(False)
        gen_layout.addWidget(self.start_gen_btn)
        
        self.stop_gen_btn = QPushButton("⏹ Остановить генерацию")
        self.stop_gen_btn.clicked.connect(self.stop_generation)
        self.stop_gen_btn.setEnabled(False)
        gen_layout.addWidget(self.stop_gen_btn)
        
        gen_group.setLayout(gen_layout)
        left_layout.addWidget(gen_group)
        
        # Настройки порогов
        threshold_group = QGroupBox("Пороговые значения")
        threshold_layout = QGridLayout()
        
        threshold_layout.addWidget(QLabel("Крит. температура:"), 0, 0)
        self.temp_threshold = QDoubleSpinBox()
        self.temp_threshold.setRange(20, 50)
        self.temp_threshold.setValue(30)
        self.temp_threshold.setSuffix(" °C")
        threshold_layout.addWidget(self.temp_threshold, 0, 1)
        
        threshold_layout.addWidget(QLabel("Комф. температура мин:"), 1, 0)
        self.comfort_temp_min = QDoubleSpinBox()
        self.comfort_temp_min.setRange(10, 30)
        self.comfort_temp_min.setValue(20)
        self.comfort_temp_min.setSuffix(" °C")
        threshold_layout.addWidget(self.comfort_temp_min, 1, 1)
        
        threshold_layout.addWidget(QLabel("Комф. температура макс:"), 2, 0)
        self.comfort_temp_max = QDoubleSpinBox()
        self.comfort_temp_max.setRange(15, 35)
        self.comfort_temp_max.setValue(26)
        self.comfort_temp_max.setSuffix(" °C")
        threshold_layout.addWidget(self.comfort_temp_max, 2, 1)
        
        threshold_layout.addWidget(QLabel("Комф. влажность мин:"), 3, 0)
        self.comfort_hum_min = QDoubleSpinBox()
        self.comfort_hum_min.setRange(10, 80)
        self.comfort_hum_min.setValue(30)
        self.comfort_hum_min.setSuffix(" %")
        threshold_layout.addWidget(self.comfort_hum_min, 3, 1)
        
        threshold_layout.addWidget(QLabel("Комф. влажность макс:"), 4, 0)
        self.comfort_hum_max = QDoubleSpinBox()
        self.comfort_hum_max.setRange(20, 90)
        self.comfort_hum_max.setValue(60)
        self.comfort_hum_max.setSuffix(" %")
        threshold_layout.addWidget(self.comfort_hum_max, 4, 1)
        
        # Кнопка обновления пороговых значений
        self.update_thresholds_btn = QPushButton("🔄 Обновить пороговые значения")
        self.update_thresholds_btn.clicked.connect(self.update_thresholds)
        self.update_thresholds_btn.setStyleSheet("background-color: #14a085; font-weight: bold;")
        threshold_layout.addWidget(self.update_thresholds_btn, 5, 0, 1, 2)
        
        threshold_group.setLayout(threshold_layout)
        left_layout.addWidget(threshold_group)
        
        # Кнопка установки
        setup_btn = QPushButton("🔧 Мастер установки")
        setup_btn.clicked.connect(self.open_setup)
        left_layout.addWidget(setup_btn)
        
        # Группа логов
        logs_group = QGroupBox("Оповещения и логи")
        logs_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        logs_layout.addWidget(self.log_text)
        
        self.clear_logs_btn = QPushButton("Очистить логи")
        self.clear_logs_btn.clicked.connect(self.clear_logs)
        logs_layout.addWidget(self.clear_logs_btn)
        
        logs_group.setLayout(logs_layout)
        left_layout.addWidget(logs_group)
        
        left_layout.addStretch()
        
        # Правая панель - вкладки комнат
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        self.tab_widget = QTabWidget()
        
        # Создание вкладок для каждой комнаты
        rooms = ['Kitchen', 'Living Room', 'Bedroom', 'Bathroom', 'Office']
        for room in rooms:
            room_widget = RoomWidget(room)
            self.room_widgets[room] = room_widget
            self.rooms_data[room] = RoomData(room)
            self.tab_widget.addTab(room_widget, room)
            
        right_layout.addWidget(self.tab_widget)
        
        # Добавление панелей в главный layout
        main_layout.addWidget(left_panel)
        main_layout.addWidget(right_panel, 1)
        
        # Применение стилей
        self.setStyleSheet(DARK_STYLE)

    def update_thresholds(self):
        """Обновление пороговых значений во всех виджетах и графиках"""
        try:
            # Получаем текущие значения из spinbox'ов
            temp_threshold = self.temp_threshold.value()
            comfort_temp_min = self.comfort_temp_min.value()
            comfort_temp_max = self.comfort_temp_max.value()
            comfort_hum_min = self.comfort_hum_min.value()
            comfort_hum_max = self.comfort_hum_max.value()
            if self.data_generator:
                self.data_generator.set_threshold(temp_threshold)
            
            # Обновляем пороги во всех виджетах комнат
            for room, widget in self.room_widgets.items():
                try:
                    widget.update_thresholds(
                        temp_threshold, 
                        comfort_temp_min, 
                        comfort_temp_max,
                        comfort_hum_min, 
                        comfort_hum_max
                    )
                except Exception as e:
                    print(f"Ошибка обновления порогов для {room}: {e}")
            
            # Логируем обновление
            self.log_message(f"✅ Пороговые значения обновлены: Т={temp_threshold}°C, "
                            f"Комф.Т={comfort_temp_min}-{comfort_temp_max}°C, "
                            f"Комф.В={comfort_hum_min}-{comfort_hum_max}%")
            
            # Показываем уведомление пользователю
            QMessageBox.information(self, "Пороги обновлены", 
                                   f"Пороговые значения успешно обновлены!\n\n"
                                   f"Критическая температура: {temp_threshold}°C\n"
                                   f"Комфортная температура: {comfort_temp_min}°C - {comfort_temp_max}°C\n"
                                   f"Комфортная влажность: {comfort_hum_min}% - {comfort_hum_max}%")
        except Exception as e:
            print(f"Ошибка в update_thresholds: {e}")
            self.log_message(f"❌ Ошибка обновления порогов: {str(e)}")
        
    def open_setup(self):
        wizard = SetupWizard(self)
        wizard.exec_()
        
    def start_kafka(self):
        """Запуск Kafka"""
        try:
            self.kafka_manager = KafkaManager()
            self.kafka_manager.status_update.connect(self.update_kafka_status)
            self.kafka_manager.start()
            
            self.start_kafka_btn.setEnabled(False)
            self.stop_kafka_btn.setEnabled(True)
            self.start_gen_btn.setEnabled(True)
        except Exception as e:
            self.log_message(f"❌ Ошибка запуска Kafka: {str(e)}")
        
    def stop_kafka(self):
        """Остановка Kafka"""
        try:
            if self.kafka_manager:
                self.kafka_manager.stop()
                self.kafka_manager = None
        except Exception as e:
            self.log_message(f"❌ Ошибка остановки Kafka: {str(e)}")
            
        self.start_kafka_btn.setEnabled(True)
        self.stop_kafka_btn.setEnabled(False)
        self.start_gen_btn.setEnabled(False)
        self.stop_gen_btn.setEnabled(False)
        
        self.kafka_status.setText("Статус: ⚫ Остановлен")
        self.kafka_status.setStyleSheet("color: #ff0000;")
        
    def update_kafka_status(self, message):
        """Обновление статуса Kafka"""
        self.log_message(message)
        if "успешно запущена" in message:
            self.kafka_status.setText("Статус: 🟢 Запущен")
            self.kafka_status.setStyleSheet("color: #00ff00;")
        elif "ошибка" in message.lower():
            self.kafka_status.setText("Статус: 🔴 Ошибка")
            self.kafka_status.setStyleSheet("color: #ff0000;")
        
    def start_generation(self):
        """Запуск генерации данных"""
        try:
            self.is_stopping = False
            self.data_generator = DataGenerator()
            self.data_generator.signals.new_data.connect(self.on_new_data)
            self.data_generator.signals.alert.connect(self.on_alert)
            self.data_generator.signals.kafka_status.connect(self.log_message)
            self.data_generator.start()
            
            self.flink_processor = FlinkProcessor()
            self.flink_processor.data_received.connect(self.on_new_data)
            self.flink_processor.alert.connect(self.on_alert)
            self.flink_processor.start()
            
            self.start_gen_btn.setEnabled(False)
            self.stop_gen_btn.setEnabled(True)
            self.log_message("🚀 Запущена генерация данных в Kafka топик 'iot-rooms'")
            
        except Exception as e:
            self.log_message(f"❌ Ошибка запуска генерации: {str(e)}")
            traceback.print_exc()
        
    def stop_generation(self):
        try:
            self.is_stopping = True
            self.log_message("⏹ Остановка генерации данных...")

            # 1. Останавливаем генератор
            if self.data_generator:
                self.data_generator.stop()

            # 2. Останавливаем Flink
            if self.flink_processor:
                self.flink_processor.stop()

            # 3. Ждём чуть-чуть (важно!)
            QApplication.processEvents()
            time.sleep(0.5)

            # 4. Теперь можно удалить
            self.data_generator = None
            self.flink_processor = None

            self.start_gen_btn.setEnabled(True)
            self.stop_gen_btn.setEnabled(False)

            self.log_message("✅ Генерация данных остановлена")

        except Exception as e:
            self.log_message(f"❌ Ошибка: {e}")
            
        # except Exception as e:
        #     self.log_message(f"❌ Ошибка при остановке генерации: {str(e)}")
        #     traceback.print_exc()
        #     # Восстанавливаем состояние кнопок даже при ошибке
        #     self.start_gen_btn.setEnabled(True)
        #     self.stop_gen_btn.setEnabled(False)
        
    def on_new_data(self, data):
        """Обработка новых данных"""
        if self.is_stopping:
            return
            
        try:
            room = data.get('room')
            if room and room in self.rooms_data:
                timestamp = datetime.fromisoformat(data['timestamp'])
                self.rooms_data[room].add_reading(
                    data['temperature'],
                    data['humidity'],
                    data['motion'],
                    timestamp
                )
                
                # Обновление виджета
                stats = {
                    'room': room,
                    'current_temp': data['temperature'],
                    'current_hum': data['humidity'],
                    'current_motion': data['motion'],
                    'avg_temp_5min': self.rooms_data[room].get_avg_temperature_5min(),
                    'avg_hum_5min': self.rooms_data[room].get_avg_humidity_5min(),
                    'has_motion_10min': self.rooms_data[room].has_motion_10min(),
                    'temp_history': self.rooms_data[room].get_temperature_history(),
                    'hum_history': self.rooms_data[room].get_humidity_history()
                }
                
                if room in self.room_widgets:
                    self.room_widgets[room].update_data(stats)
        except Exception as e:
            print(f"Ошибка обработки новых данных: {e}")
        
    def on_alert(self, room, message):
        """Обработка оповещений"""
        if self.is_stopping:
            return
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {room}: {message}"
            self.log_message(log_entry)
        except Exception as e:
            print(f"Ошибка при обработке оповещения: {e}")
        
    def log_message(self, message):
        """Добавление сообщения в лог"""
        try:
            self.log_text.append(message)
            scrollbar = self.log_text.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())
        except Exception as e:
            print(f"Ошибка при логировании: {e}")
        
    def clear_logs(self):
        """Очистка логов"""
        try:
            self.log_text.clear()
            self.alerts.clear()
        except Exception as e:
            print(f"Ошибка при очистке логов: {e}")
        
    def closeEvent(self, event):
        """Обработка закрытия окна"""
        try:
            self.is_stopping = True
            self.stop_generation()
            self.stop_kafka()
        except Exception as e:
            print(f"Ошибка при закрытии: {e}")
        event.accept()

def main():
    try:
        app = QApplication(sys.argv)
        app.setStyle('Fusion')
        
        # Настройка темной палитры
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(53, 53, 53))
        palette.setColor(QPalette.WindowText, Qt.white)
        palette.setColor(QPalette.Base, QColor(25, 25, 25))
        palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        palette.setColor(QPalette.ToolTipBase, Qt.white)
        palette.setColor(QPalette.ToolTipText, Qt.white)
        palette.setColor(QPalette.Text, Qt.white)
        palette.setColor(QPalette.Button, QColor(53, 53, 53))
        palette.setColor(QPalette.ButtonText, Qt.white)
        palette.setColor(QPalette.BrightText, Qt.red)
        palette.setColor(QPalette.Highlight, QColor(13, 115, 119))
        palette.setColor(QPalette.HighlightedText, Qt.black)
        app.setPalette(palette)
        
        window = MainWindow()
        window.show()
        
        sys.exit(app.exec_())
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()