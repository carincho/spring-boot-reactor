package com.carincho.springboot.reactor.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.carincho.springboot.reactor.app.models.Comentarios;
import com.carincho.springboot.reactor.app.models.Usuario;
import com.carincho.springboot.reactor.app.models.UsuarioComentarios;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


//CommandLineRunner para poder trabajar en consola
/**
 * 
 * Flux es un publisher por lo que es un observable
 * Cada vez que se reciba un elemento cada que el observador notifica que recibimos algo atraves del metodo do  metodo evento que parte del ciclo de vida
 * del observable cada vez que llega un elemento
 * 
 * JHay que suscribirse al flujo al observable
 * 
 * Por cada elemento del flujo se ejecuta cada vez que el observador nombres notifica que ha llegado un elemento
 * Inmutabilidad de los streams reactivos
 * Es otro flujo Flux<String>nombres y los cambios
 *
 * Cada operador se va creando otra instancia con su propio estado modificado
 * 
 */

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner{
	
	/**
	 * Inmutabilidad de los streams reactivos
	 * Es otro flujo Flux<String>nombres y los cambios
	 * 
	 * 
	 */
	
	private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
//		ejemploIterable();
//		ejemploFlatMap();
//		ejemploToString();
//		ejemploCollectionList();
//		ejemploUsuarioComentariosFlatMap();
//		ejemploUsuarioComentariosZipWith();
//		ejemploUsuarioComentariosZipWith();
//		ejemploUsuarioComentariosZipWithForma2();
//		ejemploUsuarioComentariosZipWithRangos();
//		ejemploInterval();
//		ejemploDelayElements();
//		ejemploIntervalInfinito();
//		ejemploIntervalDesdeCreate();
		ejemploContraPresion();
		
	
		
		
		/**
		 * onSubscribe([Synchronous Fuseable] FluxRange.RangeSubscription)
		 * request(unbounded)--- es cuando el suscriptor solicita cantidad de elementos a manejar o recibir
		 * 
		 * 
		 */

	}
	
	public void ejemploContraPresion() {
		
		log.info("---ejemploContraPresion---");
		
		Flux.range(1,  10)
		.log()
		.limitRate(5)//Otra forma de pedir lote
		.subscribe(
//		new Subscriber<Integer>() {
//			
//			private Subscription s;
//			private Integer limite = 5;
//			private Integer consumido = 0;
//
//			@Override
//			public void onSubscribe(Subscription s) {
//				
//				this.s = s;
////				s.request(Long.MAX_VALUE); Maximo numero de elementos
//				s.request(limite);
//				
//			}
//
//			@Override
//			public void onNext(Integer t) {
//				
//				log.info("Contrapresion: " + t.toString());
//				consumido++; //Aqui se procesa el elemento que se esta emitiendo
//				
//				if(consumido == limite) {
//					consumido = 0;
//					s.request(limite);
//				}
//				
//				
//			}
//
//			@Override
//			public void onError(Throwable t) {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public void onComplete() {
//				// TODO Auto-generated method stub
//				
//			}
			
			
//		}
	);
		
	}
	
	//Crear Observable desde 0 con create
	
	public void ejemploIntervalDesdeCreate() {
		
		log.info("---ejemploIntervalInfinito---");
		
		Flux.create(emitter -> {
			
			Timer time = new  Timer();
			time.schedule(new TimerTask() {
				
				private Integer contador = 0;
				@Override
				public void run() {
					
					//Finalizar con exito
					emitter .next(++contador);//Guardamos el dato que se va a emitir en el observable
					if(contador == 10) {
						time.cancel();//cancelar el timer
						emitter.complete();// terminar el emmiter solo, el onComplete solo se llama cuando todo ocurrio bien
					}
					
//					Finalizar con error
					if(contador == 5) {
						emitter.error(new InterruptedException("Error, se ha detenido el flux en 5"));
						time.cancel();
						
					}
					
				}
			}, 1000, 1000);
			
			
		})
//		.doOnNext(next -> log.info(next.toString()))
//		.doOnComplete(() -> log.info("Se acaboooo...."))//Para dar un mensaje cuando haya terminado
//		.subscribe();
		.subscribe(next -> log.info(next.toString()),
				error -> log.error(error.getMessage()),
				()-> log.info("Se acaboooo..."));//Otra forma de hacerlo con el suscribe de 3 argumentos
		
		
	}
	
	
	//Intervalos infinitos con interval y operador delayElements
	
	public void ejemploIntervalInfinito() throws InterruptedException {
		
		log.info("---ejemploIntervalInfinito---");
		
		CountDownLatch latch = new CountDownLatch(1);//Para hacer el delay y ver el resultado
		
		Flux.interval(Duration.ofSeconds(1))
		.doOnTerminate(latch::countDown) //en evento aqui se decrementa para que llegue a cero, siempre se ejecuta falle o no falle, si no ocurre un error se va a terminar hasta que termine el observable
		//Si ocurre un error va a terminar el observable se invoca doOnterminate se ejecuta y el await se libera
		.flatMap(i -> {
			
			if(i >= 5) {
				return Flux.error(new NullPointerException("Solo 5 valores!!"));
			} 
			
			return Flux.just(i);
			
		})//la idea es obtener cada valor y lo vamos a cambiar por otro observable del tipo error
		.map(i -> "CONTEO " + i)
		.retry(2)//Va a intentar ejecutar el flujo n veces cada vez que falla, util para evitar error
//		.doOnNext(s -> log.info(s))
		.subscribe(s -> log.info(s), e -> log.error(e.getMessage()));
		
		latch.await();//Va a estar esperando hasta que el contador llegue a cero se decrementa
		
		
	}
	
	
//	Intervalos de tiempo con interval y zipWith
	public void ejemploDelayElements() throws InterruptedException {
		
		log.info("---ejemploDelayElements---");
		
		Flux<Integer> rango = Flux.range(1, 12)
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(i -> log.info(i.toString()));
		
		rango.subscribe();
//		rango.blockLast();//No es recomenable por que puede provocar cuellos de botella ya que la idea es trabajar sin bloqueos
		
//		Thread.sleep(13000);
		
		
	}
	
	/**
	 * 
	 * 
	 * Finaliza este flux se hace en segundo plano se sigue ejecutando en la maquina virtual
	 * Se sigue ejecutando es sin bloque no bloquea las tareas en la maquina virtual se esta ejecutando
	 * en distintos hilos 
	 * 
	 * Para ejemplo podemos forzar para que sea por bloqueo
	 * 
	 * 
	 */
	public void ejemploInterval() {
		
		log.info("---ejemploInterval---");
		
		Flux<Integer> rango = Flux.range(1, 12);
		
		Flux<Long>delay = Flux.interval(Duration.ofSeconds(1));
		
		rango.zipWith(delay, (ra, de) -> ra)
		.doOnNext(i -> log.info(i.toString()))//doOnNext es para aplicar una tarea
//		.subscribe();
		.blockLast();//Bloquea hasta el ultimo elemento que se emite, suscribe pero bloquea
		
	}
	
	
	
	
	
	/**
	 * Range se usa para obtener un Flux de un rango determinado
	 * 
	 */
	public void ejemploUsuarioComentariosZipWithRangos() {
		
		log.info("---ejemploUsuarioComentariosZipWithRangos---");
		
		Flux.just(1, 2, 3, 4)
		.map(i -> (i*2))
		.zipWith(Flux.range(0, 4), (uno, dos) -> String.format("Primer flux: %d, Segundo flux: %d", uno, dos))
		.subscribe(log::info);
		
		
		
	}
	public void ejemploUsuarioComentariosZipWithForma2() {
		
		log.info("---ejemploUsuarioComentariosZipWithForma2---");
		
		Mono<Usuario>usuarioMono = Mono.fromCallable(() -> new Usuario("John", "Doe"));
		Mono<Comentarios>comentariosUsuarioMono = Mono.fromCallable(() -> {
			Comentarios comentarios =  new Comentarios();
			
			comentarios.addComentario("John Doe eres el ejemplo");
			comentarios.addComentario("Muy comun en los ambientes de prueba");
			comentarios.addComentario("Pero tambien tienes una esposa");
			comentarios.addComentario("Jane Doe");
			
			return comentarios;
		});
		
		//usuarioMonoflujo principal y se combina con otro flujo comentariosUsuarioMono
		
		Mono<UsuarioComentarios> usuarioComentarios =  usuarioMono
				.zipWith(comentariosUsuarioMono)
				.map(tuple -> {
					
					Usuario u = tuple.getT1();
					Comentarios c = tuple.getT2();
					
					return new UsuarioComentarios(u, c);
					
				});
		usuarioComentarios.subscribe(uc -> log.info(uc.toString()));
		
	}
	
	//Combiar flujos con zipWith
public void ejemploUsuarioComentariosZipWith() {
		
		log.info("---ejemploUsuarioComentariosZipWith---");
		
		Mono<Usuario>usuarioMono = Mono.fromCallable(() -> new Usuario("John", "Doe"));
		Mono<Comentarios>comentariosUsuarioMono = Mono.fromCallable(() -> {
			Comentarios comentarios =  new Comentarios();
			
			comentarios.addComentario("John Doe eres el ejemplo");
			comentarios.addComentario("Muy comun en los ambientes de prueba");
			comentarios.addComentario("Pero tambien tienes una esposa");
			comentarios.addComentario("Jane Doe");
			
			return comentarios;
		});
		
		//usuarioMonoflujo principal y se combina con otro flujo comentariosUsuarioMono
		
		Mono<UsuarioComentarios> usuarioComentarios =  usuarioMono.zipWith(comentariosUsuarioMono, (usuario, comentariosUsuario) -> new UsuarioComentarios(usuario, comentariosUsuario));
		usuarioComentarios.subscribe(uc -> log.info(uc.toString()));
		
	}
	/**
	 * 
	 * Combiar dos flujos con operador flatMap
	 * 
	 */
	
	public void ejemploUsuarioComentariosFlatMap() {
		
		log.info("---ejemploUsuarioComentariosFlatMap---");
		
		Mono<Usuario>usuarioMono = Mono.fromCallable(() -> new Usuario("John", "Doe"));
		Mono<Comentarios>comentariosUsuarioMono = Mono.fromCallable(() -> {
			Comentarios comentarios =  new Comentarios();
			
			comentarios.addComentario("John Doe eres el ejemplo");
			comentarios.addComentario("Muy comun en los ambientes de prueba");
			comentarios.addComentario("Pero tambien tienes una esposa");
			comentarios.addComentario("Jane Doe");
			
			return comentarios;
		});
		
		usuarioMono.flatMap(u -> comentariosUsuarioMono.map(c -> new UsuarioComentarios(u, c)))
		.subscribe(uc -> log.info(uc.toString()));
		
	}
	
	
	/**
	 * 
	 * convertir Flux a Mono
	 * 
	 * Con sl suscribe es un Flux imprime uno por uno
	 * 
	 * 
	 */
	
	public void ejemploCollectionList() throws Exception {
		
		log.info("---ejemploCollectionList---");
		
		//FlatMap convierte a otro tipo de flujo mono o Flux y por debajo
		
		List<Usuario>usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario ("Oscar", "delAngel"));
		usuariosList.add(new Usuario ("Guillermo", "Morales"));
		usuariosList.add(new Usuario ("Terecua", "De la Rosa"));
		usuariosList.add(new Usuario ("Caro", "delAngel"));
		usuariosList.add(new Usuario ("Coral", "Morales"));
		usuariosList.add(new Usuario ("Sharon", "Diaz"));
		usuariosList.add(new Usuario ("Rubi", "Diaz"));
		usuariosList.add(new Usuario ("Oscar", "Diaz"));
		
//		Flux lista de usuarios
//		Flux.fromIterable(usuariosList)
//		.subscribe(u -> log.info(u.toString())); 
		
		
		// Convertir a Mono un solo objeto
		
		Flux.fromIterable(usuariosList)
		.collectList()
		.subscribe(lista -> {
			
			lista.forEach(item -> log.info(item.toString()));
		});
		
		
	}
	public void ejemploToString() throws Exception {
			
			log.info("---ejemploToString---");
			
			//FlatMap convierte a otro tipo de flujo mono o Flux y por debajo
			
			List<Usuario>usuariosList = new ArrayList<>();
			usuariosList.add(new Usuario ("Oscar", "delAngel"));
			usuariosList.add(new Usuario ("Guillermo", "Morales"));
			usuariosList.add(new Usuario ("Terecua", "De la Rosa"));
			usuariosList.add(new Usuario ("Caro", "delAngel"));
			usuariosList.add(new Usuario ("Coral", "Morales"));
			usuariosList.add(new Usuario ("Sharon", "Diaz"));
			usuariosList.add(new Usuario ("Rubi", "Diaz"));
			usuariosList.add(new Usuario ("Oscar", "Diaz"));
				
			Flux.fromIterable(usuariosList)
					.map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toUpperCase()))
					.flatMap(nombre -> {
						if(nombre.contains("oscar".toUpperCase())) {
							return Mono.just(nombre);//Tiene que ser del tipo observable un mono, modificando obteniendo un observable
						} else {
							
							return Mono.empty();
						}
					})
					.map(nombre -> {
						
						return nombre.toLowerCase();
					})
					.subscribe(u -> log.info(u.toString())); 
			
			}
	
	public void ejemploFlatMap() throws Exception {
		
		log.info("---ejemploFlatMap---");
		
		//FlatMap convierte a otro tipo de flujo mono o Flux y por debajo
		
		List<String>usuariosList = new ArrayList<>();
		usuariosList.add("Oscar delAngel");
		usuariosList.add("Guillermo Morales");
		usuariosList.add("Terecua De la Rosa");
		usuariosList.add("Caro del Angel");
		usuariosList.add("Coral Morales");
		usuariosList.add("Sharon Diaz");
		usuariosList.add("Rubi Diaz");
		usuariosList.add("Oscar Diaz");
			
		Flux.fromIterable(usuariosList)
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.flatMap(usuario -> {
					if(usuario.getNombre().equalsIgnoreCase("oscar")) {
						return Mono.just(usuario);//Tiene que ser del tipo observable un mono, modificando obteniendo un observable
					} else {
						
						return Mono.empty();
					}
				})
				.map(usuario -> {
					
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				})
				.subscribe(u -> log.info(u.toString())); 
		
		}

	public void ejemploIterable() throws Exception {
		
		log.info("---ejemploIterable---");
		
		//Creando un flux Observable a partir de un List o Iterable Los list se usan para base de datos relacionales que regresar list no reactivos
	//	Las BD no relacionales regresan ya un Flux
		
		List<String>usuariosList = new ArrayList<>();
		usuariosList.add("Oscar delAngel");
		usuariosList.add("Guillermo Morales");
		usuariosList.add("Terecua De la Rosa");
		usuariosList.add("Caro del Angel");
		usuariosList.add("Coral Morales");
		usuariosList.add("Sharon Diaz");
		usuariosList.add("Rubi Diaz");
		usuariosList.add("Oscar Diaz");
		
	//	Flux<String>nombres = Flux.just("Oscar del Angel","Guillermo Morales","Terecua De la Rosa","Caro del Angel","Coral Morales","Sharon Diaz","Rubi Diaz", "Oscar Diaz");
		Flux<String>nombres = Flux.fromIterable(usuariosList);
		
		
		
		
		Flux<Usuario> usuarios = nombres.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter(usuario -> usuario.getNombre().toLowerCase().equals("oscar"))
				.doOnNext(usuario -> {
					if(usuario == null) {
						throw new RuntimeException("Nombres no pueden ser vacios");
					} 
						System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				})
				.map(usuario -> {
					
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});
				
		
		usuarios.subscribe(e -> log.info(e.toString()),
				error -> log.error(error.getMessage()),
				new Runnable() {
					
					@Override
					public void run() {
						log.info("Ha finalizado la ejecucion del observable con exito!");
						
					}
				});//Cuando suscribimos estamos observando se trata de un observador ejecuta algun tipo de tarea 
		
		}
}
