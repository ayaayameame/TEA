"""
Module: main_window.py
Description: TEA Intraday Overlay Terminal.
             - Fixed Abscissa: 0 to 24 hours (Yesterday vs Today).
             - Zoom Disabled: Static professional view.
             - High Precision: Granular Y-ticks starting at 0.
             - Current Price Marker: Dynamic symbol and tag at the current hour.
"""
import sys
from pathlib import Path
from datetime import datetime, time, timedelta

# --- PROJECT PATH HACK ---
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import json
import numpy as np
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QLineEdit, QPushButton, QLabel,
                               QComboBox, QCompleter)
from PySide6.QtCore import Qt
import pyqtgraph as pg

from src.database.connector import TarkovDatabase

class TEAMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.db_path = ROOT_DIR / "data" / "tarkov_economy.db"
        self.mapping_path = ROOT_DIR / "data" / "items_mapping.json"
        
        self.db = TarkovDatabase(str(self.db_path))
        self.item_db = self.load_item_mapping()
        self.current_df = None
        self.view_mode = "lowest" 

        self.setWindowTitle("TEA - Intraday Overlay Terminal")
        self.resize(1200, 800)
        
        self.setup_ui()
        self.apply_theme()
        self.setup_completer()
        self.connect_signals()

    def load_item_mapping(self):
        if self.mapping_path.exists():
            with open(self.mapping_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def setup_ui(self):
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        
        # --- Top Bar ---
        self.top_bar = QHBoxLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["PvP Market", "PvE Market"])
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search item (e.g. Moonshine)...")
        self.search_btn = QPushButton("Analyze")
        self.search_btn.setObjectName("AnalyzeBtn")
        
        self.top_bar.addWidget(QLabel("Mode:"))
        self.top_bar.addWidget(self.mode_combo)
        self.top_bar.addWidget(self.search_box)
        self.top_bar.addWidget(self.search_btn)
        self.top_bar.addStretch()
        self.main_layout.addLayout(self.top_bar)

        # --- View Toggles ---
        self.btn_layout = QHBoxLayout()
        self.btn_lowest = QPushButton("Lowest Price")
        self.btn_average = QPushButton("Average Price")
        for b in [self.btn_lowest, self.btn_average]:
            b.setCheckable(True)
            b.setFixedWidth(200)
        self.btn_lowest.setChecked(True)
        self.btn_layout.addWidget(self.btn_lowest)
        self.btn_layout.addWidget(self.btn_average)
        self.main_layout.addLayout(self.btn_layout)

        # --- Plot Widget (No DateAxisItem, using standard numbers 0-24) ---
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('#11111b')
        self.plot_widget.showGrid(x=True, y=True, alpha=0.5)
        self.plot_widget.setLabel('left', "Price (kR)", color='#cdd6f4')
        self.plot_widget.setLabel('bottom', "Hour of Day (00:00 - 24:00)", color='#cdd6f4')
        
        # [REQUIREMENT 1]: Disable Zoom and Pan
        self.plot_widget.setMouseEnabled(x=False, y=False)
        self.plot_widget.hideButtons() # Remove the 'A' button
        
        self.main_layout.addWidget(self.plot_widget)
        self.vb = self.plot_widget.getViewBox()

    def setup_completer(self):
        if not self.item_db: return
        self.completer = QCompleter(list(self.item_db.keys()), self)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.setFilterMode(Qt.MatchContains)
        self.search_box.setCompleter(self.completer)

    def connect_signals(self):
        self.search_btn.clicked.connect(self.handle_search)
        self.search_box.returnPressed.connect(self.handle_search)
        self.btn_lowest.clicked.connect(lambda: self.switch_view("lowest"))
        self.btn_average.clicked.connect(lambda: self.switch_view("average"))

    def switch_view(self, mode):
        self.view_mode = mode
        self.btn_lowest.setChecked(mode == "lowest")
        self.btn_average.setChecked(mode == "average")
        if self.current_df is not None:
            self.render_chart()

    def handle_search(self):
        query = self.search_box.text().strip()
        target_id = None
        for name, item_id in self.item_db.items():
            if query.lower() in name.lower():
                target_id = item_id
                self.search_box.setText(name)
                break
        
        if target_id:
            mode = "pvp" if "PvP" in self.mode_combo.currentText() else "pve"
            # Always fetch 24h for overlay
            df = self.db.get_price_history(target_id, "24 Hours", mode)
            if df is not None and not df.empty:
                self.current_df = df
                self.render_chart()
            else:
                self.statusBar().showMessage("No data found.")

    def render_chart(self):
        """Implements the fixed 0-24h overlay logic."""
        if self.current_df is None: return
        self.plot_widget.clear()
        
        # 1. Identify Calendar Boundaries
        now_local = datetime.now()
        start_today = datetime.combine(now_local.date(), time.min)
        start_yesterday = start_today - timedelta(days=1)
        
        # 2. Split Data into Yesterday and Today
        df = self.current_df
        mask_yesterday = (df['dt_local'] >= start_yesterday) & (df['dt_local'] < start_today)
        mask_today = (df['dt_local'] >= start_today)
        
        df_yesterday = df[mask_yesterday].copy()
        df_today = df[mask_today].copy()
        
        # Helper to convert local DT to hour-of-day float (0.0 to 24.0)
        def get_hour_float(dt_series, start_dt):
            return (dt_series.view(np.int64) - start_dt.timestamp() * 1e9) / (3600 * 1e9)

        if self.view_mode == "lowest":
            col, color_main = 'lowest_price', '#a6e3a1' # Green
        else:
            col, color_main = 'avg_price', '#89b4fa' # Blue
        
        # 3. Plot Yesterday (Reference Line - Dimmed)
        if not df_yesterday.empty:
            x_yest = get_hour_float(df_yesterday['dt_local'], start_yesterday)
            y_yest = df_yesterday[col].values / 1000.0
            self.plot_widget.plot(x_yest, y_yest, pen=pg.mkPen('#45475a', width=1.5, style=Qt.DashLine))
        
        # 4. Plot Today (Active Line - Bold)
        if not df_today.empty:
            x_today = get_hour_float(df_today['dt_local'], start_today)
            y_today = df_today[col].values / 1000.0
            self.plot_widget.plot(x_today, y_today, pen=pg.mkPen(color_main, width=3),
                                 fillLevel=0, brush=(*pg.mkColor(color_main).getRgb()[:3], 30))
            
            # CURRENT PRICE SYMBOL & TAG
            now_x = x_today.iloc[-1]
            now_y = y_today[-1]
            
            # Horizontal Price Marker
            h_line = pg.InfiniteLine(pos=now_y, angle=0, pen=pg.mkPen(color_main, width=1, style=Qt.DashLine))
            self.plot_widget.addItem(h_line)
            # Circle Dot
            self.plot_widget.plot([now_x], [now_y], pen=None, symbol='o', symbolBrush=color_main, symbolSize=10)
            # Price Tag
            tag = pg.TextItem(text=f" {now_y:.1f}k", color=color_main, anchor=(0, 0.5))
            tag.setPos(now_x, now_y)
            self.plot_widget.addItem(tag)

        # 5. FIXED COORDINATE SYSTEM (0 to 24)
        self.plot_widget.setXRange(0, 24, padding=0)
        
        y_max = df[col].max() / 1000.0 if not df.empty else 100
        self.plot_widget.setYRange(0, y_max * 1.15, padding=0)
        
        # Set Tick Spacing for Precision
        spacing = 1.0 if y_max < 50 else 10.0 if y_max < 500 else 50.0
        self.plot_widget.getAxis('left').setTickSpacing(spacing, spacing/5)

    def apply_theme(self):
        self.setStyleSheet("""
            QMainWindow, QWidget { background-color: #1e1e2e; color: #cdd6f4; font-family: 'Segoe UI'; }
            QLineEdit, QComboBox { background-color: #313244; border: 1px solid #45475a; border-radius: 4px; padding: 5px; color: #cdd6f4; }
            QPushButton:checked { background-color: #89b4fa; color: #11111b; font-weight: bold; }
            QPushButton#AnalyzeBtn { background-color: #a6e3a1; color: #11111b; font-weight: bold; }
        """)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TEAMainWindow()
    window.show()
    sys.exit(app.exec())