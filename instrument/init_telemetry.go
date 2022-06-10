package instrument

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func Init(serviceName string) (func(), error) {

	log.Printf("[WARN] instrument.Init service '%s'", serviceName)
	ctx := context.Background()

	// check whether a telemetry endpoint is configured
	otelAgentAddr, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		log.Printf("[WARN] OTEL_EXPORTER_OTLP_ENDPOINT not set - returning")
		// return empty shutdown func
		return func() {}, nil
		//otelAgentAddr = "localhost:4317"
	}

	log.Printf("[TRACE] init telemetry, endpoint: %s", otelAgentAddr)

	//metricClient := otlpmetricgrpc.NewClient(
	//	otlpmetricgrpc.WithInsecure(),
	//	otlpmetricgrpc.WithEndpoint(otelAgentAddr))
	//metricExp, err := otlpmetric.New(ctx, metricClient)
	//handleErr(err, "Failed to create the collector metric exporter")

	//pusher := controller.New(
	//	processor.NewFactory(
	//		simple.NewWithHistogramDistribution(),
	//		metricExp,
	//	),
	//	controller.WithExporter(metricExp),
	//	controller.WithCollectPeriod(2*time.Second),
	//)
	//global.SetMeterProvider(pusher)

	//err = pusher.Start(ctx)
	//handleErr(err, "Failed to start metric pusher")

	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otelAgentAddr),
		// TODO telemetry test what happens to traces before the server has connected
		// TODO telemetry heartbeat?
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)

	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create the collector trace exporter: %s", err.Error())
	}

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
		log.Printf("[WARN] failed to create resource: %s", err.Error())
		return nil, fmt.Errorf("failed to create resource: %s", err.Error())
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(tracerProvider)

	shutdown := func() {
		log.Printf("[TRACE] shutdown telemetry ")

		// TODO not sure why this is necessary - maybe because of batching?
		tracerProvider.ForceFlush(context.Background())

		cxt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := traceExp.Shutdown(cxt); err != nil {
			log.Printf("[WARN] error occurred durint telemtry shutdown: %s", err.Error())
			otel.Handle(err)
		}

		// pushes any last exports to the receiver
		//if err := pusher.Stop(cxt); err != nil {
		//	otel.Handle(err)
		//}
	}
	log.Printf("[TRACE] init telemetry end")

	return shutdown, nil
}
