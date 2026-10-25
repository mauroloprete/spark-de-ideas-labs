# Estilo compartido de los gráficos del bench (lo usan graficos.R y live.R).

library(ggplot2)

navy <- "#1e293b"; orange <- "#FFA166"; bg <- "#f8fafc"
gray <- "#64748b"; gris_claro <- "#94a3b8"

# RT naranja, serverless gris, cache violeta. PRO = serverless en CSVs viejos.
tipo_map <- c(REALTIME         = "Lakehouse//RT",
              SERVERLESS       = "SQL Serverless",
              PRO              = "SQL Serverless",
              SERVERLESS_CACHE = "SQL Serverless (cache)")
colores_tipo <- c("Lakehouse//RT"          = orange,
                  "SQL Serverless"         = gray,
                  "SQL Serverless (cache)" = "#593196")

query_map <- c(q1_estado_cliente = "Q1 · lookup puntual",
               q2_top_productos  = "Q2 · top productos 24 h",
               q3_panel_segmento = "Q3 · join por segmento")

caption <- "⚡ Spark de Ideas · mauroloprete.github.io/mauroloprete"

tema_spark <- function() {
  theme_minimal(base_size = 11) +
    theme(plot.background = element_rect(fill = bg, color = NA),
          panel.background = element_rect(fill = bg, color = NA),
          panel.grid.major.x = element_blank(),
          plot.title = element_text(color = navy, size = 12, face = "bold"),
          axis.text = element_text(color = navy, size = 9),
          axis.title = element_text(color = gray, size = 8),
          strip.text = element_text(color = navy, size = 9, face = "bold"),
          legend.position = "bottom",
          legend.text = element_text(color = navy, size = 8),
          plot.caption = element_text(color = gris_claro, size = 7,
                                      hjust = 1, face = "italic"),
          plot.margin = margin(10, 12, 8, 12))
}

# Filtra el crudo y arma los factores (series y queries en orden fijo).
# Devuelve list(d = datos, colores = vector nombrado para fill/color).
preparar_bench <- function(d) {
  d <- d[d$estado == "SUCCEEDED" & d$query != "baseline_select_1", ]
  lbl <- unname(tipo_map[d$tipo])
  niveles_wh <- intersect(names(colores_tipo), unique(lbl))
  d$wh <- factor(lbl, levels = niveles_wh)
  colores <- colores_tipo[niveles_wh]

  d$q <- factor(query_map[d$query], levels = unname(query_map))
  d$total_time_ms <- suppressWarnings(as.numeric(d$total_time_ms))
  list(d = d, colores = colores)
}
