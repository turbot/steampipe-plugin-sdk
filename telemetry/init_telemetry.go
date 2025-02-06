package telemetry

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

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
	"google.golang.org/grpc/credentials/insecure"
)

func Init(serviceName string) (func(), error) {

	ctx := context.Background()

	// is telemetry enabled
	telemetryEnvStr := strings.ToLower(os.Getenv(EnvOtelLevel))
	tracingEnabled := slices.Contains([]string{OtelAll, OtelTrace}, telemetryEnvStr)
	metricsEnabled := slices.Contains([]string{OtelAll, OtelMetrics}, telemetryEnvStr)

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

	var opts grpc.DialOption
	if _, ok := os.LookupEnv(EnvOtelInsecure); ok {
		log.Printf("[TRACE] STEAMPIPE_OTEL_INSECURE is set - disable security checks")
		opts = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		opts = grpc.EmptyDialOption{}
	}
	grpcConn, err := grpc.DialContext(ctx, otelAgentAddr, opts)
	if err != nil {
		return nil, err
	}

	log.Printf("[TRACE] otel endpoint: %s", otelAgentAddr)
	var traceExp *otlptrace.Exporter
	var tracerProvider *sdktrace.TracerProvider

	var metricReader sdkmetric.Reader
	var meterProvider *sdkmetric.MeterProvider

	if tracingEnabled {
		traceExp, tracerProvider, err = initTracing(ctx, grpcConn, serviceName)
		if err != nil {
			return nil, err
		}
	}

	if metricsEnabled {
		metricReader, meterProvider, err = initMetrics(ctx, grpcConn, serviceName)
		if err != nil {
			return nil, err
		}
	}

	// create a callback function which can be called when telemetry needs to shut down
	shutdown := func() {
		log.Printf("[TRACE] shutdown telemetry ")
		// we are giving a timeout of 2 seconds here. this is inline with the timeout that
		// the grpc plugin library uses during plugin shutdown
		// worst case is that things wont flush by then - nothing to worry about
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if tracerProvider != nil && traceExp != nil {
			// flush batched data
			log.Printf("[TRACE] shutdown tracer")
			if err := tracerProvider.ForceFlush(ctx); err != nil {
				log.Printf("[TRACE] could not flush during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			if err := traceExp.Shutdown(ctx); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			log.Printf("[TRACE] shutdown tracer complete")
		}

		if meterProvider != nil && metricReader != nil {
			log.Printf("[TRACE] shutdown metrics")
			// flush batched data
			if err := meterProvider.ForceFlush(ctx); err != nil {
				log.Printf("[TRACE] could not flush during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			// pushes any last exports to the receiver
			if err := metricReader.Shutdown(ctx); err != nil {
				log.Printf("[TRACE] error occurred during telemetry shutdown: %s", err.Error())
				otel.Handle(err)
			}
			log.Printf("[TRACE] shutdown metrics complete")
		}

		// close the GRPC connection
		if grpcConn != nil {
			log.Printf("[TRACE] shutdown grpc connection")
			grpcConn.Close()
			log.Printf("[TRACE] shutdown grpc connection complete")
		}
		log.Printf("[TRACE] shutdown telemetry complete")
	}
	log.Printf("[TRACE] init telemetry end")

	return shutdown, nil
}

// initMetrics initializes OpenTelemetry metrics SDK and exporters which push data over the given GRPC connection
func initMetrics(ctx context.Context, grpcConnection *grpc.ClientConn, serviceName string) (sdkmetric.Reader, *sdkmetric.MeterProvider, error) {
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

	// create a metric reader which exports out to the GRPC connection every 60 seconds
	reader := sdkmetric.NewPeriodicReader(exporter)

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)

	otel.SetMeterProvider(provider)

	return reader, provider, nil
}

// initTracing initializes the OpenTelemetry tracing/span SDK and exporters which push data over the given GRPC connection
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

// getResource creates a resource that we can set to the TracerProvider and MeterProvider
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
