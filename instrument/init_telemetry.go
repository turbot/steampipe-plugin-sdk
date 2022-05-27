package instrument

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/turbot/steampipe/constants"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

func Init() (func(), error) {

	ctx := context.Background()

	// TODO HACKED IP FOR NOW
	// TODO timeout connection to collector
	otelAgentAddr, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		otelAgentAddr = "localhost:4317"
	}
	log.Printf("[TRACE] init telemetry")

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
		otlptracegrpc.WithDialOption(grpc.WithBlock()))
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
			semconv.ServiceNameKey.String(constants.AppName),
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
