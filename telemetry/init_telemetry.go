package telemetry

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/turbot/go-kit/helpers"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

func Init(serviceName string) (func(), error) {
	log.Printf("[TRACE] instrument.Init service '%s'", serviceName)
	ctx := context.Background()

	// is telemetry enabled
	telemetryEnvStr := strings.ToLower(os.Getenv(EnvOtelLevel))
	tracingEnabled := helpers.StringSliceContains([]string{OtelAll, OtelTrace}, telemetryEnvStr)
	metricsEnabled := helpers.StringSliceContains([]string{OtelAll, OtelMetrics}, telemetryEnvStr)
	if !tracingEnabled && !metricsEnabled {
		log.Printf("[TRACE] instrument.Init: metrics and tracing disabled' - returning")
		// return empty shutdown func
		return func() {}, nil
	}

	// check whether a telemetry endpoint is configured
	otelAgentAddr, endpointSet := os.LookupEnv(EnvOtelEndpoint)
	if !endpointSet {
		log.Printf("[TRACE] instrument.Init: OTEL_EXPORTER_OTLP_ENDPOINT not set - returning")
		// return empty shutdown func
		return func() {}, nil
		//otelAgentAddr = "localhost:4317"
	}

	log.Printf("[TRACE] init telemetry, endpoint: %s", otelAgentAddr)

	var pusher *controller.Controller
	var traceExp *otlptrace.Exporter
	var tracerProvider *sdktrace.TracerProvider
	var err error

	if metricsEnabled {
		pusher, err = initMetrics(ctx, otelAgentAddr)
		if err != nil {
			// return empty shutdown func
			return nil, err
		}
	}
	log.Printf("[TRACE] create client")

	if tracingEnabled {
		traceExp, tracerProvider, err = initTracing(ctx, otelAgentAddr, serviceName)
		if err != nil {
			return nil, err
		}
	}
	shutdown := func() {
		log.Printf("[TRACE] shutdown telemetry ")
		cxt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		if tracingEnabled {
			// TODO not sure why this is necessary - maybe because of batching?
			tracerProvider.ForceFlush(context.Background())
			if err := traceExp.Shutdown(cxt); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
		}
		if metricsEnabled {
			// pushes any last exports to the receiver
			if err := pusher.Stop(cxt); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
		}
	}
	log.Printf("[TRACE] init telemetry end")

	return shutdown, nil
}

func initMetrics(ctx context.Context, otelAgentAddr string) (*controller.Controller, error) {
	metricClient := otlpmetricgrpc.NewClient(
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(otelAgentAddr))
	metricExp, err := otlpmetric.New(ctx, metricClient)
	if err != nil {
		log.Printf("[TRACE] initMetrics: failed to create the collector metric exporter: %s", err.Error())
		return nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}

	pusher := controller.New(
		processor.NewFactory(
			simple.NewWithHistogramDistribution(),
			metricExp,
		),
		controller.WithExporter(metricExp),
		controller.WithCollectPeriod(2*time.Second),
	)
	global.SetMeterProvider(pusher)

	if err := pusher.Start(ctx); err != nil {
		log.Printf("[TRACE] initMetrics: failed to start metric pusher: %s", err.Error())
		return nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}
	return pusher, nil
}

func initTracing(ctx context.Context, otelAgentAddr, serviceName string) (*otlptrace.Exporter, *sdktrace.TracerProvider, error) {
	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otelAgentAddr),
		otlptracegrpc.WithDialOption(grpc.WithBlock(), grpc.WithTimeout(5*time.Second)),
	)
	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		log.Printf("[TRACE] initTracing: error creating trace exporter: %v", err)
		if strings.LastIndex(err.Error(), "context deadline exceeded") != -1 {
			err = fmt.Errorf("timeout connecting to listener at %s", otelAgentAddr)
		}
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}

	log.Printf("[TRACE] got exporter")

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		log.Printf("[TRACE] initTracing: failed to create resource: %s", err.Error())
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}
	log.Printf("[TRACE] got resource")

	bsp := sdktrace.NewBatchSpanProcessor(traceExp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	log.Printf("[TRACE] got tracerProvider")

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(tracerProvider)
	return traceExp, tracerProvider, nil
}
