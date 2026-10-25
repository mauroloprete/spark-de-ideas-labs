# Gráficos del bench a partir del crudo (latencias.csv): server-side por
# query, p95 por concurrencia y wall-clock del cliente. Deja en consola el
# resumen p50/p95 que va a la tabla del post.
#
# Uso: Rscript bench/graficos.R [ruta_csv] [dir_salida]

f <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
dir_bench <- if (length(f)) dirname(sub("^--file=", "", f[1])) else "bench"
source(file.path(dir_bench, "estilo.R"))

args <- commandArgs(trailingOnly = TRUE)
ruta_csv <- if (length(args) >= 1) args[1] else
  file.path(dir_bench, "results", "latencias.csv")
dir_out  <- if (length(args) >= 2) args[2] else dirname(ruta_csv)

prep <- preparar_bench(read.csv(ruta_csv))
d <- prep$d
colores <- prep$colores

# 1. Server-side a concurrencia 1: la latencia del motor, sin la red.
srv <- d[d$concurrencia == 1 & !is.na(d$total_time_ms), ]
p1 <- ggplot(srv, aes(x = q, y = total_time_ms, fill = wh)) +
  geom_point(aes(color = wh), position = position_jitterdodge(
    jitter.width = 0.12, dodge.width = 0.65),
    alpha = 0.25, size = 1, show.legend = FALSE) +
  geom_boxplot(alpha = 0.65, outlier.shape = NA, color = navy,
               linewidth = 0.4, width = 0.55,
               position = position_dodge(width = 0.65)) +
  scale_fill_manual(values = colores, name = NULL) +
  scale_color_manual(values = colores) +
  labs(x = NULL, y = "latencia server-side por query (ms)",
       caption = caption) +
  tema_spark()
ggsave(file.path(dir_out, "grafico_servidor.png"), p1,
       width = 9, height = 4, dpi = 150, bg = bg)

# 2. p95 server-side por nivel de concurrencia: cómo aguanta cada uno la carga.
celdas <- aggregate(total_time_ms ~ wh + q + concurrencia,
                    data = d[!is.na(d$total_time_ms), ],
                    FUN = quantile, probs = 0.95)
names(celdas)[names(celdas) == "total_time_ms"] <- "p95"
p2 <- ggplot(celdas, aes(x = factor(concurrencia), y = p95, fill = wh)) +
  geom_col(position = position_dodge(width = 0.7, preserve = "single"),
           width = 0.6) +
  geom_text(aes(label = round(p95)),
            position = position_dodge(width = 0.7), vjust = -0.4,
            size = 2.8, color = navy, fontface = "bold") +
  facet_wrap(~ q, nrow = 1) +
  scale_fill_manual(values = colores, name = NULL) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.12))) +
  labs(x = "requests en paralelo", y = "p95 server-side (ms)",
       caption = caption) +
  tema_spark()
ggsave(file.path(dir_out, "grafico_concurrencia.png"), p2,
       width = 9, height = 3.6, dpi = 150, bg = bg)

# 3. Wall-clock del cliente: lo que ve una app desde Montevideo (motor + red).
cli <- d[d$concurrencia == 1, ]
p3 <- ggplot(cli, aes(x = q, y = client_ms, fill = wh)) +
  geom_point(aes(color = wh), position = position_jitterdodge(
    jitter.width = 0.12, dodge.width = 0.65),
    alpha = 0.25, size = 1, show.legend = FALSE) +
  geom_boxplot(alpha = 0.65, outlier.shape = NA, color = navy,
               linewidth = 0.4, width = 0.55,
               position = position_dodge(width = 0.65)) +
  scale_fill_manual(values = colores, name = NULL) +
  scale_color_manual(values = colores) +
  labs(x = NULL, y = "wall-clock del cliente por query (ms)",
       caption = caption) +
  tema_spark()
ggsave(file.path(dir_out, "grafico_cliente.png"), p3,
       width = 9, height = 4, dpi = 150, bg = bg)

# Resumen para la tabla del post.
resumen <- aggregate(cbind(total_time_ms, client_ms) ~ wh + q + concurrencia,
                     data = d[!is.na(d$total_time_ms), ],
                     FUN = function(x) c(p50 = quantile(x, 0.5),
                                         p95 = quantile(x, 0.95)))
resumen <- do.call(data.frame, resumen)
names(resumen) <- c("warehouse", "query", "conc",
                    "srv_p50", "srv_p95", "cli_p50", "cli_p95")
resumen[, 4:7] <- round(resumen[, 4:7])
print(resumen[order(resumen$query, resumen$conc, resumen$warehouse), ],
      row.names = FALSE)

cat("\nPNG en", dir_out, "\n")
