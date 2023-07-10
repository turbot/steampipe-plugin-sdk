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
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

func Init(serviceName string) (func(), error) {
	ctx := context.Background()

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Println("[TRACE] Otel handled error", err)
	}))

	// is telemetry enabled
	telemetryEnvStr := strings.ToLower(os.Getenv(EnvOtelLevel))
	tracingEnabled := helpers.StringSliceContains([]string{OtelAll, OtelTrace}, telemetryEnvStr)
	metricsEnabled := helpers.StringSliceContains([]string{OtelAll, OtelMetrics}, telemetryEnvStr)

	log.Printf("[TRACE] telemetry.Init service '%s', tracingEnabled: %v, metricsEnabled: %v", serviceName, tracingEnabled, metricsEnabled)

	if !tracingEnabled && !metricsEnabled {
		log.Printf("[TRACE] metrics and tracing disabled' - returning")
		// return empty shutdown func
		return func() {}, nil
	}

	// check whether a telemetry endpoint is configured
	otelAgentAddr, endpointSet := os.LookupEnv(EnvOtelEndpoint)
	if !endpointSet {
		log.Printf("[TRACE] OTEL_EXPORTER_OTLP_ENDPOINT not set - defaulting to 'localhost:4317'")
		otelAgentAddr = "localhost:4317"
	}

	grpcConn, err := grpc.DialContext(ctx, otelAgentAddr)

	if err != nil {
		// return empty shutdown func
		return nil, err
	}

	log.Printf("[TRACE] endpoint: %s", otelAgentAddr)
	var traceExp *otlptrace.Exporter
	var tracerProvider *sdktrace.TracerProvider

	var metrixExp sdkmetric.Exporter
	var meterProvider *sdkmetric.MeterProvider

	if metricsEnabled {
		metrixExp, meterProvider, err = initMetrics(ctx, grpcConn, serviceName)
		if err != nil {
			// return empty shutdown func
			return nil, err
		}
	}
	if tracingEnabled {
		traceExp, tracerProvider, err = initTracing(ctx, grpcConn, serviceName)
		if err != nil {
			return nil, err
		}
	}
	shutdown := func() {
		log.Printf("[TRACE] shutdown telemetry ")
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		if tracingEnabled {
			// flush batched data
			if err := tracerProvider.ForceFlush(context.Background()); err != nil {
				log.Printf("[TRACE] could not flush during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			if err := traceExp.Shutdown(ctx); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
		}
		if metricsEnabled {
			// flush batched data
			if err := meterProvider.ForceFlush(context.Background()); err != nil {
				log.Printf("[TRACE] could not flush during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			// pushes any last exports to the receiver
			if err := metrixExp.Shutdown(ctx); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
		}

		if tracingEnabled || metricsEnabled {
			grpcConn.Close()
		}
	}
	log.Printf("[TRACE] init telemetry end")

	return shutdown, nil
}

func initMetrics(ctx context.Context, grpcConnection *grpc.ClientConn, serviceName string) (sdkmetric.Exporter, *sdkmetric.MeterProvider, error) {
	log.Printf("[TRACE] telemetry.initMetrics")
	res, err := getResource(ctx, serviceName)
	if err != nil {
		log.Printf("[TRACE] initTracing: failed to create resource: %s", err.Error())
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}
	log.Printf("[TRACE] got resource")

	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(grpcConnection))
	if err != nil {
		log.Printf("[TRACE] initMetrics: failed to create the collector metric exporter: %s", err.Error())
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter),
		),
	)

	otel.SetMeterProvider(provider)

	return exporter, provider, nil
}

func initTracing(ctx context.Context, grpcConn *grpc.ClientConn, serviceName string) (*otlptrace.Exporter, *sdktrace.TracerProvider, error) {
	res, err := getResource(ctx, serviceName)
	if err != nil {
		log.Printf("[TRACE] initTracing: failed to create resource: %s", err.Error())
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}
	log.Printf("[TRACE] got resource")

	traceExp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(grpcConn))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialise Open Telemetry: %s", err.Error())
	}
	log.Printf("[TRACE] got exporter")

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

func getResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String(serviceName),
		),
	)
}
