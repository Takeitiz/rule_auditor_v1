"""
Spectral Analysis Strategy using FFT and Lomb-Scargle Periodogram
"""

import numpy as np
from scipy import signal
from scipy.signal import lombscargle
from typing import Optional, Dict, List
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult


class SpectralAnalysisStrategy(TimezoneStrategy):
    """
    Detect timezone using FFT and Lomb-Scargle periodogram for irregular sampling.
    Identifies 24-hour periodicities and phase shifts.
    """

    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate timezone suggestion using spectral analysis"""

        if timezone:
            # Analyze specific timezone
            spectral_score = self._analyze_timezone_spectrum(statistics, timezone)
            if spectral_score > 0.6:
                result = TimezoneResult(timezone=timezone)
                result.method_used = "spectral_analysis"
                result.confidence = spectral_score
                return result
            return None

        # Analyze all timezones
        best_tz = None
        max_power = 0
        spectral_details = {}

        for tz in statistics.count_30_min_distribution.keys():
            # Analyze spectrum for this timezone
            score, details = self._analyze_timezone_spectrum_detailed(statistics, tz)

            if score > max_power:
                max_power = score
                best_tz = tz

            spectral_details[tz] = details

        if best_tz and max_power > 0.6:  # Threshold for significance
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "spectral_analysis"
            result.confidence = float(max_power)

            # Add detailed reasoning
            details = spectral_details[best_tz]
            result.suggest_reason = (
                f"Strong 24h periodicity (power={details['daily_power']:.3f}), "
                f"weekly pattern={details['weekly_power']:.3f}, "
                f"phase={details['phase_hours']:.1f}h"
            )
            return result

        return None

    def _analyze_timezone_spectrum(self, statistics: StatisticsResult, timezone: str) -> float:
        """Analyze spectral characteristics for a single timezone"""
        score, _ = self._analyze_timezone_spectrum_detailed(statistics, timezone)
        return score

    def _analyze_timezone_spectrum_detailed(self, statistics: StatisticsResult, timezone: str) -> Tuple[float, Dict]:
        """Detailed spectral analysis returning score and details"""

        # Prepare time series data
        times, values = self._prepare_time_series(statistics, timezone)

        if len(times) < 10:  # Need minimum data points
            return 0, {}

        # Use Lomb-Scargle for irregular sampling
        daily_power, weekly_power, phase = self._lomb_scargle_analysis(times, values)

        # Also try FFT if data is regular enough
        fft_score = self._fft_analysis(statistics, timezone)

        # Combine scores
        combined_score = (daily_power * 0.6 + weekly_power * 0.2 + fft_score * 0.2)

        details = {
            'daily_power': daily_power,
            'weekly_power': weekly_power,
            'phase_hours': phase,
            'fft_score': fft_score,
            'combined_score': combined_score
        }

        return combined_score, details

    def _prepare_time_series(self, statistics: StatisticsResult, timezone: str) -> Tuple[np.ndarray, np.ndarray]:
        """Convert distribution to time series for spectral analysis"""

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist:
            return np.array([]), np.array([])

        times = []
        values = []

        # Convert 30-min buckets to continuous time series
        for bucket, count in sorted(time_dist.items()):
            hour = int(bucket[:2])
            minute = 30 if bucket[2:] == '30' else 0
            time_hours = hour + minute/60
            times.append(time_hours)
            values.append(count)

        # Extend to multiple days if needed for better frequency resolution
        extended_times = []
        extended_values = []

        for day in range(7):  # Extend to a week
            for t, v in zip(times, values):
                extended_times.append(t + day * 24)
                # Add some variation for different days
                weekday_factor = 1.0 if day < 5 else 0.3  # Lower activity on weekends
                extended_values.append(v * weekday_factor)

        return np.array(extended_times), np.array(extended_values)

    def _lomb_scargle_analysis(self, times: np.ndarray, values: np.ndarray) -> Tuple[float, float, float]:
        """
        Perform Lomb-Scargle periodogram analysis.
        Returns (daily_power, weekly_power, phase_hours)
        """

        if len(times) < 10:
            return 0, 0, 0

        # Normalize data
        values = (values - np.mean(values)) / (np.std(values) + 1e-10)

        # Frequency grid focusing on daily and weekly periods
        # Daily frequency: 1/24 hours^-1
        # Weekly frequency: 1/168 hours^-1
        freq_daily = np.linspace(0.035, 0.045, 200)  # Around 1/24
        freq_weekly = np.linspace(0.005, 0.007, 100)  # Around 1/168

        # Calculate periodogram for daily pattern
        try:
            # Use scipy's lombscargle
            angular_freq_daily = 2 * np.pi * freq_daily
            pgram_daily = lombscargle(times, values, angular_freq_daily, normalize=True)

            # Find peak power
            daily_peak_idx = np.argmax(pgram_daily)
            daily_power = pgram_daily[daily_peak_idx] / np.max([1.0, np.max(pgram_daily)])
            daily_freq = freq_daily[daily_peak_idx]

            # Calculate phase from the peak frequency
            phase = self._calculate_phase(times, values, daily_freq)

            # Weekly pattern
            angular_freq_weekly = 2 * np.pi * freq_weekly
            pgram_weekly = lombscargle(times, values, angular_freq_weekly, normalize=True)
            weekly_peak_idx = np.argmax(pgram_weekly)
            weekly_power = pgram_weekly[weekly_peak_idx] / np.max([1.0, np.max(pgram_weekly)])

        except Exception as e:
            # Fallback to simple periodicity detection
            daily_power = self._simple_periodicity_score(values, 48)  # 48 half-hours in a day
            weekly_power = 0
            phase = 0

        return daily_power, weekly_power, phase

    def _calculate_phase(self, times: np.ndarray, values: np.ndarray, frequency: float) -> float:
        """Calculate phase shift for a given frequency"""

        # Fit sinusoidal model: y = A*sin(2π*f*t + φ) + C
        omega = 2 * np.pi * frequency

        # Use least squares to find phase
        sin_component = np.sin(omega * times)
        cos_component = np.cos(omega * times)

        # Solve for coefficients
        A = np.column_stack([sin_component, cos_component, np.ones_like(times)])
        coeffs, _, _, _ = np.linalg.lstsq(A, values, rcond=None)

        # Calculate phase from sin and cos coefficients
        phase = np.arctan2(coeffs[1], coeffs[0])

        # Convert phase to hours
        phase_hours = (phase / (2 * np.pi)) * (1 / frequency)

        # Normalize to 0-24 hour range
        phase_hours = phase_hours % 24

        return phase_hours

    def _fft_analysis(self, statistics: StatisticsResult, timezone: str) -> float:
        """
        Perform FFT analysis for regular time series.
        Returns normalized power at 24-hour frequency.
        """

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist:
            return 0

        # Create regular time series (48 points for 24 hours)
        time_series = np.zeros(48)

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            minute = 30 if bucket[2:] == '30' else 0
            idx = hour * 2 + (1 if minute == 30 else 0)
            if idx < 48:
                time_series[idx] = count

        # Apply window to reduce spectral leakage
        window = signal.windows.hamming(len(time_series))
        windowed_series = time_series * window

        # Compute FFT
        fft_result = np.fft.fft(windowed_series)
        freqs = np.fft.fftfreq(len(windowed_series))

        # Find power at daily frequency (1 cycle per series)
        daily_freq_idx = 1  # 1 cycle per 48 samples = 1 day

        if daily_freq_idx < len(fft_result):
            daily_power = np.abs(fft_result[daily_freq_idx])**2
            total_power = np.sum(np.abs(fft_result)**2)

            if total_power > 0:
                normalized_power = daily_power / total_power
                return float(normalized_power)

        return 0

    def _simple_periodicity_score(self, values: np.ndarray, period: int) -> float:
        """Simple periodicity detection as fallback"""

        if len(values) < period:
            return 0

        # Calculate autocorrelation at the specified period
        if len(values) >= 2 * period:
            correlation = np.corrcoef(values[:period], values[period:2*period])[0, 1]
            return max(0, correlation)

        return 0