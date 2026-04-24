import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { ChangeDetectorRef, Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { API_BASE_URL } from '../../core/api-base';

export type TranscriptTopicStatus = 'covered' | 'partial' | 'missing';

export interface TranscriptAnalysisTopic {
  title: string;
  status: TranscriptTopicStatus;
  evidence?: string;
  timestamp?: string;
  note?: string;
}

export interface TranscriptKeyMoment {
  timestamp?: string;
  summary: string;
}

export interface TranscriptAnalysisResult {
  coverageScore: number;
  overallAssessment: string;
  topics: TranscriptAnalysisTopic[];
  extraTopics: string[];
  keyMoments: TranscriptKeyMoment[];
  strengths: string[];
  gaps: string[];
  pedagogyNotes?: string;
  language?: string;
  meta: {
    model: string;
    tookMs: number;
    transcriptChars: number;
    syllabusChars: number;
    promptChars: number;
    courseLabel?: string;
    sessionLabel?: string;
    teacherLabel?: string;
    generatedAt: string;
  };
}

@Component({
  selector: 'app-transcript-analysis-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './transcript-analysis.page.html',
  styleUrl: './transcript-analysis.page.css',
})
export class TranscriptAnalysisPageComponent {
  loading = false;
  error = '';
  result: TranscriptAnalysisResult | null = null;
  showRawTranscript = false;

  form = {
    apiKey: '',
    courseLabel: '',
    sessionLabel: '',
    teacherLabel: '',
    syllabusText: '',
    transcriptText: '',
  };

  private readonly API_KEY_STORAGE = 'uai.geminiApiKey';

  constructor(
    private readonly http: HttpClient,
    private readonly cdr: ChangeDetectorRef,
  ) {
    try {
      const saved = localStorage.getItem(this.API_KEY_STORAGE);
      if (saved) this.form.apiKey = saved;
    } catch {
      /* ignore */
    }
  }

  rememberApiKey() {
    try {
      if (this.form.apiKey) {
        localStorage.setItem(this.API_KEY_STORAGE, this.form.apiKey);
      } else {
        localStorage.removeItem(this.API_KEY_STORAGE);
      }
    } catch {
      /* ignore */
    }
  }

  forgetApiKey() {
    this.form.apiKey = '';
    try {
      localStorage.removeItem(this.API_KEY_STORAGE);
    } catch {
      /* ignore */
    }
  }

  get canSubmit() {
    return (
      !this.loading &&
      this.form.apiKey.trim().length >= 10 &&
      this.form.syllabusText.trim().length >= 10 &&
      this.form.transcriptText.trim().length >= 10
    );
  }

  get coveredCount() {
    return this.result?.topics.filter((t) => t.status === 'covered').length ?? 0;
  }
  get partialCount() {
    return this.result?.topics.filter((t) => t.status === 'partial').length ?? 0;
  }
  get missingCount() {
    return this.result?.topics.filter((t) => t.status === 'missing').length ?? 0;
  }

  statusLabel(status: TranscriptTopicStatus) {
    switch (status) {
      case 'covered':
        return 'Cubierto';
      case 'partial':
        return 'Parcial';
      case 'missing':
        return 'No abordado';
    }
  }

  statusClass(status: TranscriptTopicStatus) {
    return `pill pill-${status}`;
  }

  coverageClass() {
    const s = this.result?.coverageScore ?? 0;
    if (s >= 75) return 'coverage-high';
    if (s >= 45) return 'coverage-mid';
    return 'coverage-low';
  }

  loadSample() {
    this.form.courseLabel = 'MAT-201 - Calculo Diferencial';
    this.form.sessionLabel = 'Semana 5 - Derivadas y regla de la cadena';
    this.form.teacherLabel = '';
    this.form.syllabusText = [
      'Sesion 5: Derivadas.',
      '- Definicion formal de derivada.',
      '- Reglas basicas (suma, producto, cociente).',
      '- Regla de la cadena.',
      '- Ejemplos con funciones polinomicas y trigonometricas.',
      '- Aplicacion: pendiente de recta tangente.',
    ].join('\n');
    this.form.transcriptText = [
      '[00:00:12] Hoy vamos a ver derivadas. La derivada mide la tasa de cambio.',
      '[00:05:40] La definicion formal es el limite del cociente incremental.',
      '[00:18:22] Reglas: suma, resta, producto. Veamos ejemplos en el pizarron.',
      '[00:35:10] Ahora aplicamos la regla de la cadena en f(g(x)).',
      '[00:50:00] Para cerrar, calculemos la pendiente de la tangente a y = x^2 en x=3.',
    ].join('\n');
  }

  clearForm() {
    const keepKey = this.form.apiKey;
    this.form = {
      apiKey: keepKey,
      courseLabel: '',
      sessionLabel: '',
      teacherLabel: '',
      syllabusText: '',
      transcriptText: '',
    };
    this.result = null;
    this.error = '';
  }

  analyze() {
    if (!this.canSubmit) {
      this.error = 'Pega la API key de Gemini, el syllabus y el transcript (minimo 10 caracteres cada uno).';
      return;
    }
    this.loading = true;
    this.error = '';
    this.result = null;
    this.cdr.detectChanges();

    this.http
      .post<TranscriptAnalysisResult>(`${API_BASE_URL}/transcript-analysis/run`, {
        apiKey: this.form.apiKey.trim(),
        syllabusText: this.form.syllabusText,
        transcriptText: this.form.transcriptText,
        courseLabel: this.form.courseLabel || undefined,
        sessionLabel: this.form.sessionLabel || undefined,
        teacherLabel: this.form.teacherLabel || undefined,
      })
      .subscribe({
        next: (res) => {
          this.result = res;
          this.loading = false;
          this.cdr.detectChanges();
        },
        error: (err) => {
          this.loading = false;
          this.error =
            err?.error?.message ||
            err?.message ||
            'Error al llamar al servicio de analisis. Revisa la consola.';
          this.cdr.detectChanges();
        },
      });
  }

  print() {
    window.print();
  }
}
