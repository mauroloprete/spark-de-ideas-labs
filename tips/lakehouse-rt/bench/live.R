# Panel en vivo del bench: corre junto a bench.py y grafica a medida que
# llegan corridas (las métricas server-side demoran ~15-30 s en aparecer).
# Vistas: boxplots cliente/servidor filtrados por concurrencia, y un gráfico
# por nivel con selector (distribución, brecha RT vs serverless, p50/p95/p99).
#
# Uso: Rscript bench/live.R [ruta_csv]   → abre http://localhost:4321

library(shiny)
library(plotly)

f <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
dir_bench <- if (length(f)) dirname(sub("^--file=", "", f[1])) else "bench"
source(file.path(dir_bench, "estilo.R"))

args <- commandArgs(trailingOnly = TRUE)
ruta_csv <- if (length(args) >= 1) {
  args[1]
} else {
  file.path(dir_bench, "results", "latencias.csv")
}

ui <- fluidPage(
  tags$head(tags$style(HTML(sprintf(
    "
    body { background: %s; color: %s;
           font-family: -apple-system, 'Segoe UI', sans-serif; }
    h3 { font-weight: 700; margin-bottom: 4px; }
    .estado { color: %s; margin-bottom: 8px; font-size: 14px; }
    .contenedor { max-width: 1100px; margin: 0 auto; padding-top: 10px; }
    .controles { display: flex; gap: 28px; margin-bottom: 6px; font-size: 13px; }
    .controles .form-group, .controles .checkbox { margin: 0; }
    .pista { color: %s; font-size: 12px; margin: -2px 0 6px; }
  ",
    bg,
    navy,
    gray,
    gray
  )))),
  div(
    class = "contenedor",
    h3("Benchmark Lakehouse//RT vs SQL Serverless"),
    div(class = "estado", textOutput("estado", inline = TRUE)),
    div(
      class = "controles",
      selectInput(
        "conc_sel",
        "Concurrencia (dos gráficos de arriba)",
        choices = NULL,
        width = "230px",
        selectize = FALSE
      ),
      checkboxInput("escala_log", "Escala logarítmica (log10)", FALSE),
      checkboxInput(
        "escalas_libres",
        "Liberar escalas para comparar dentro de cada query",
        FALSE
      )
    ),
    plotlyOutput("p_cliente", height = 330),
    plotlyOutput("p_servidor", height = 330),
    div(
      class = "controles",
      style = "margin-top: 10px;",
      radioButtons(
        "vista_conc",
        NULL,
        choices = c(
          "Distribución completa",
          "Brecha RT vs Serverless",
          "p50",
          "p95",
          "p99"
        ),
        selected = "Distribución completa",
        inline = TRUE
      )
    ),
    conditionalPanel(
      "input.vista_conc == 'Brecha RT vs Serverless'",
      div(
        class = "pista",
        HTML(sprintf(
          paste(
            "<b style='color:%s'>Lakehouse//RT</b> vs",
            "<b style='color:%s'>SQL Serverless</b> · cada línea es un nivel de",
            "concurrencia; la parte de izquierda de la mancuerna va del p50",
            "(punto grande) al p99 (cola). ×N = mediana del Serverless ÷ mediana",
            "del RT (×8 = el Serverless tarda 8 veces más en el caso típico)."
          ),
          orange,
          gray
        ))
      )
    ),
    plotlyOutput("p_concurrencia", height = 420)
  )
)

server <- function(input, output, session) {
  crudo <- reactivePoll(
    1000,
    session,
    checkFunc = function() {
      if (file.exists(ruta_csv)) file.mtime(ruta_csv) else -1
    },
    valueFunc = function() {
      if (!file.exists(ruta_csv)) {
        return(NULL)
      }
      tryCatch(read.csv(ruta_csv), error = function(e) NULL)
    }
  )

  prep <- reactive({
    d <- crudo()
    validate(need(!is.null(d) && nrow(d) > 0, "Esperando latencias.csv"))
    pr <- preparar_bench(d)
    validate(need(
      nrow(pr$d) > 0,
      "Baseline y warm-up en curso, esperando las primeras queries"
    ))
    pr
  })

  # Actualiza el selector solo cuando aparece un nivel nuevo, para no pisar
  # la selección en cada refresco del CSV.
  niveles_previos <- reactiveVal(NULL)
  observe({
    pr <- prep()
    niveles <- sort(unique(pr$d$concurrencia))
    if (identical(niveles, niveles_previos())) {
      return()
    }
    niveles_previos(niveles)
    sel <- isolate(input$conc_sel)
    if (is.null(sel) || !(sel %in% as.character(niveles))) {
      sel <- as.character(min(niveles))
    }
    updateSelectInput(session, "conc_sel", choices = niveles, selected = sel)
  })

  # Sin selector poblado todavía, cae al nivel mínimo.
  conc_actual <- function(pr) {
    sel <- suppressWarnings(as.integer(input$conc_sel))
    if (length(sel) == 1 && !is.na(sel) && sel %in% pr$d$concurrencia) {
      sel
    } else {
      min(pr$d$concurrencia)
    }
  }

  # puntos_mudos apaga el hover de los puntos jitter: repiten lo de la caja.
  al_plotly <- function(p, puntos_mudos = FALSE) {
    gp <- ggplotly(p)
    if (puntos_mudos) {
      marcadores <- vapply(gp$x$data, function(tr) {
        identical(tr$type, "scatter") &&
          !is.null(tr$mode) && grepl("markers", tr$mode)
      }, logical(1))
      if (any(marcadores)) {
        gp <- style(gp, hoverinfo = "skip", traces = which(marcadores))
      }
    }
    gp %>%
      config(displayModeBar = FALSE) %>%
      layout(
        boxmode = "group",
        paper_bgcolor = bg,
        plot_bgcolor = bg,
        font = list(family = "-apple-system, 'Segoe UI', sans-serif"),
        legend = list(
          orientation = "h",
          x = 0.5,
          xanchor = "center",
          y = -0.22,
          title = list(text = "")
        ),
        margin = list(t = 45)
      )
  }

  boxplot_vivo <- function(datos, y, colores, titulo, eje_y) {
    p <- ggplot(datos, aes(x = q, y = .data[[y]], fill = wh)) +
      geom_point(
        aes(color = wh),
        position = position_jitterdodge(
          jitter.width = 0.12,
          dodge.width = 0.65
        ),
        alpha = 0.25,
        size = 1,
        show.legend = FALSE
      ) +
      geom_boxplot(
        alpha = 0.65,
        outlier.shape = NA,
        color = navy,
        linewidth = 0.4,
        width = 0.55,
        position = position_dodge(width = 0.65, preserve = "single")
      ) +
      scale_fill_manual(values = colores, name = NULL) +
      scale_color_manual(values = colores) +
      labs(title = titulo, x = NULL, y = eje_y) +
      tema_spark()
    # scales = "free" también libera el x discreto: cada faceta, su query.
    if (isTRUE(input$escalas_libres)) {
      p <- p +
        facet_wrap(~q, nrow = 1, scales = "free") +
        theme(axis.text.x = element_blank())
    }
    if (isTRUE(input$escala_log)) {
      p <- p + scale_y_log10()
    }
    al_plotly(p, puntos_mudos = TRUE)
  }

  output$estado <- renderText({
    d0 <- crudo()
    req(!is.null(d0), nrow(d0) > 0)
    con_srv <- sum(!is.na(suppressWarnings(as.numeric(d0$total_time_ms))))
    ult <- d0[nrow(d0), ]
    sprintf(
      "%d corridas · métricas server-side %d/%d · última: %s · %s @ %s",
      nrow(d0),
      con_srv,
      nrow(d0),
      ult$warehouse,
      ult$query,
      ult$concurrencia
    )
  })

  output$p_cliente <- renderPlotly({
    pr <- prep()
    csel <- conc_actual(pr)
    cli <- pr$d[pr$d$concurrencia == csel, ]
    validate(need(nrow(cli) > 0, "Sin corridas todavía"))
    boxplot_vivo(
      cli,
      "client_ms",
      pr$colores,
      sprintf("Wall-clock del cliente (red incluida) · concurrencia %d", csel),
      "client_ms"
    )
  })

  output$p_servidor <- renderPlotly({
    pr <- prep()
    csel <- conc_actual(pr)
    srv <- pr$d[pr$d$concurrencia == csel & !is.na(pr$d$total_time_ms), ]
    validate(need(
      nrow(srv) > 0,
      "Esperando métricas server-side del Query History (demoran 15 a 30 segundos)"
    ))
    boxplot_vivo(
      srv,
      "total_time_ms",
      pr$colores,
      sprintf("Latencia server-side (solo el motor) · concurrencia %d", csel),
      "total_time_ms"
    )
  })

  # Gráfico por concurrencia, con selector de vista.
  output$p_concurrencia <- renderPlotly({
    pr <- prep()
    dd <- pr$d[!is.na(pr$d$total_time_ms), ]
    validate(need(nrow(dd) > 0, "Esperando métricas server-side"))
    if (input$vista_conc == "Distribución completa") {
      p <- ggplot(
        dd,
        aes(x = factor(concurrencia), y = total_time_ms, fill = wh)
      ) +
        geom_point(
          aes(color = wh),
          position = position_jitterdodge(
            jitter.width = 0.12,
            dodge.width = 0.65
          ),
          alpha = 0.2,
          size = 0.8,
          show.legend = FALSE
        ) +
        geom_boxplot(
          alpha = 0.65,
          outlier.shape = NA,
          color = navy,
          linewidth = 0.4,
          width = 0.55,
          position = position_dodge(width = 0.65, preserve = "single")
        ) +
        scale_color_manual(values = pr$colores) +
        labs(
          title = "Tiempos server-side por corrida y nivel de concurrencia",
          y = "total_time_ms"
        )
      if (isTRUE(input$escala_log)) p <- p + scale_y_log10()
    } else if (startsWith(input$vista_conc, "Brecha")) {
      # Brecha: una fila por query, una sub-línea por nivel, y dos cometas
      # p50→p99 por serie; ×N anota la razón de medianas contra el RT.
      qs <- aggregate(
        total_time_ms ~ wh + tipo + q + concurrencia,
        data = dd,
        FUN = quantile,
        probs = c(0.5, 0.95, 0.99)
      )
      qs <- do.call(data.frame, qs)
      names(qs)[5:7] <- c("p50", "p95", "p99")
      centros <- rev(seq_along(levels(qs$q))) # Q1 en la fila de arriba
      niveles_c <- sort(unique(qs$concurrencia))
      frac <- (match(qs$concurrencia, niveles_c) - 1) /
        max(1, length(niveles_c) - 1)
      qs$yn <- centros[as.integer(qs$q)] + 0.3 - 0.6 * frac
      largo <- do.call(
        rbind,
        lapply(c("p50", "p95", "p99"), function(pc) {
          cbind(qs[c("wh", "yn")], percentil = pc, ms = qs[[pc]])
        })
      )
      largo$percentil <- factor(largo$percentil, c("p50", "p95", "p99"))
      lineas <- aggregate(p50 ~ q + concurrencia + yn, data = qs, FUN = min)
      # Etiquetas según la escala: offset proporcional en log, fijo en lineal.
      lineas$x_lbl <- if (isTRUE(input$escala_log)) {
        lineas$p50 / 1.6
      } else {
        lineas$p50 - max(qs$p99) * 0.012
      }
      rt <- qs[qs$tipo == "REALTIME", c("q", "concurrencia", "yn", "p50")]
      # El ×N se mide contra el serverless SIN cache.
      sv <- qs[qs$tipo %in% c("SERVERLESS", "PRO"), c("q", "concurrencia", "p50")]
      brecha <- merge(
        rt,
        sv,
        by = c("q", "concurrencia"),
        suffixes = c("_rt", "_sv")
      )
      brecha$razon <- brecha$p50_sv / brecha$p50_rt
      brecha$x_lbl <- if (isTRUE(input$escala_log)) {
        sqrt(brecha$p50_rt * brecha$p50_sv)
      } else {
        (brecha$p50_rt + brecha$p50_sv) / 2
      }
      # Sin leyenda: el color se explica en la línea de ayuda de la UI.
      p <- ggplot(qs, aes(y = yn, group = wh)) +
        geom_linerange(
          aes(xmin = p50, xmax = p99, color = wh),
          linewidth = 3.5,
          alpha = 0.3,
          show.legend = FALSE
        ) +
        geom_point(
          data = largo,
          aes(x = ms, color = wh, size = percentil, alpha = percentil),
          show.legend = FALSE
        ) +
        geom_text(
          data = lineas,
          aes(x = x_lbl, y = yn, label = concurrencia),
          inherit.aes = FALSE,
          color = gris_claro,
          size = 2.8,
          hjust = 1
        ) +
        scale_size_manual(values = c(p50 = 3.2, p95 = 2.1, p99 = 1.3)) +
        scale_alpha_manual(values = c(p50 = 1, p95 = 0.75, p99 = 0.55)) +
        scale_color_manual(values = pr$colores, guide = "none") +
        scale_y_continuous(breaks = centros, labels = levels(qs$q)) +
        labs(
          title = "La brecha: p50 → p99 bajo carga",
          x = if (isTRUE(input$escala_log)) {
            "latencia server-side (ms, escala log10)"
          } else {
            "latencia server-side (ms)"
          },
          y = NULL
        ) +
        tema_spark() +
        theme(
          panel.grid.major.x = element_line(color = "#e2e8f0", linewidth = 0.3),
          panel.grid.major.y = element_blank(),
          legend.position = "none",
          axis.text.y = element_text(face = "bold")
        )
      if (isTRUE(input$escala_log)) {
        p <- p + scale_x_log10()
      }
      if (nrow(brecha) > 0) {
        p <- p +
          geom_text(
            data = brecha,
            aes(
              x = x_lbl,
              y = yn + 0.1,
              label = ifelse(
                razon >= 10,
                sprintf("×%.0f", razon),
                sprintf("×%.1f", razon)
              )
            ),
            inherit.aes = FALSE,
            color = navy,
            fontface = "bold",
            size = 3.2
          )
      }
    } else {
      prob <- as.numeric(sub("^p", "", input$vista_conc)) / 100
      celdas <- aggregate(
        total_time_ms ~ wh + q + concurrencia,
        data = dd,
        FUN = quantile,
        probs = prob
      )
      names(celdas)[names(celdas) == "total_time_ms"] <- "valor"
      p <- ggplot(celdas, aes(x = factor(concurrencia), y = valor, fill = wh)) +
        geom_col(
          position = position_dodge(width = 0.7, preserve = "single"),
          width = 0.6
        ) +
        labs(
          title = sprintf(
            "%s server-side por nivel de concurrencia",
            input$vista_conc
          ),
          y = sprintf("%s (ms)", input$vista_conc)
        )
      p <- p +
        (if (isTRUE(input$escala_log)) {
          scale_y_log10()
        } else {
          scale_y_continuous(expand = expansion(mult = c(0, 0.12)))
        })
    }
    # La vista Brecha arma sus propias facetas y tema; el resto comparte estos.
    if (!startsWith(input$vista_conc, "Brecha")) {
      p <- p +
        facet_wrap(
          ~q,
          nrow = 1,
          scales = if (isTRUE(input$escalas_libres)) "free_y" else "fixed"
        ) +
        scale_fill_manual(values = pr$colores, name = NULL) +
        labs(x = "requests en paralelo") +
        tema_spark()
    }
    al_plotly(p, puntos_mudos = input$vista_conc == "Distribución completa")
  })
}

runApp(shinyApp(ui, server), port = 4321, launch.browser = TRUE)
