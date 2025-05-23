package com.servidor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main implements Runnable {
    private static final Integer SERVER_COUNT = 5;

    private static final String TASK_QUEUE_NAME = "FILA_MENSAGEM";
    private static final String EXCHANGE_NAME = "REPLICAR_SERVIDORES";

    private final String ArquivoJSON;
    
    private final Map<String, Object> Dados = new HashMap<>(); 
    private final ObjectMapper MapeadorDados = new ObjectMapper();
    
    private final int ThreadID;
    private long RelogioLogico;

    private static final String SYNC_EXCHANGE = "CLOCK_SYNC_CALL";
    private static final String ELECTION_QUEUE_PREFIX = "ELECTION_VOTE_CALL";

    private volatile boolean isCoordinator = false;
    private volatile long LastCoordinatorPing = System.currentTimeMillis();
    private final long ElectionTriggerTimeout = 10000; // 10 segundos sem ping
    private static final AtomicBoolean ElectionInProgress = new AtomicBoolean(false);
    private volatile boolean AwaitingElectionResponse = false;
    private static final Object CoordinatorLock = new Object();

    private volatile boolean isRunning = true;
    private Thread CoordinatorThread = null;

    private PrintWriter logWriter;

    public Main(int threadId) {
        this.ThreadID = threadId;
        this.ArquivoJSON = "/data/banco_servidor_id_" + threadId + ".json";
    }

    public void EscutarCanalSincronizador(Channel CanalSincronizacao, String syncQueue) throws IOException {
        try {
            CanalSincronizacao.basicConsume(syncQueue, true, (consumerTag, delivery) -> {
                if (!isCoordinator) {
                    String RelogioString = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    long RelogioRecebido = Long.parseLong(RelogioString);
                    VerificarRelogioEAtualizar(RelogioRecebido, true);
                    LastCoordinatorPing = System.currentTimeMillis();
                }
            }, consumerTag -> {
            });
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
    
    public void EscutarCanalDeEleicao(Channel CanalEleicao, Channel CanalEleicaoFinal, Connection ConexaoFinal,
            Channel CanalSincronizacaoFinal, String NomeFilaEleicao) throws IOException {
        try {
            CanalEleicao.basicConsume(NomeFilaEleicao, true, (consumerTag, delivery) -> {
                String body = new String(delivery.getBody(), StandardCharsets.UTF_8);

                if (body.startsWith("ELECTION_FROM_")) {
                    int ThreadIDOriginaria = Integer.parseInt(body.split("_")[2]);
                    RegistrarEvento("ELECTION EVENT RECEIVED FROM SERVER " + ThreadIDOriginaria);
                    
                    if (ThreadID > ThreadIDOriginaria) {
                        AwaitingElectionResponse = false; 
                        
                        // Se ainda não estou em uma eleição, eu inicio
                        if (ElectionInProgress.compareAndSet(false, true)) {
                            IniciarEleicao(CanalEleicaoFinal, ConexaoFinal, CanalSincronizacaoFinal);
                        } else {
                            RegistrarEvento("ELECTION ONGOING");
                        }

                    }
                } else if (body.startsWith("NEW_COORDINATOR_")) {
                    int coord = Integer.parseInt(body.split("_")[2]);
                    RegistrarEvento("ELECTED NEW COORDINATOR: SERVER " + coord);
                    
                    synchronized (CoordinatorLock) {
                        AwaitingElectionResponse = false;
                        
                        if (coord == ThreadID && !isCoordinator) {
                            isCoordinator = true;
                            IniciarCoordenador(CanalSincronizacaoFinal);
                        } else if (isCoordinator && coord != ThreadID) {
                            PararCoordenador();
                            isCoordinator = false;
                        } else if (coord != ThreadID) {
                            isCoordinator = false;
                        }
                        
                        // Reset do estado de eleição
                        ElectionInProgress.set(false);
                        LastCoordinatorPing = System.currentTimeMillis();
                    }
                }
            }, consumerTag -> {
        });
        } catch(IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void run() {
        this.RelogioLogico = System.currentTimeMillis() / 1000L; // segundos desde época
        AtualizarRelogio();

        try {
            File logFile = new File("/data/log_eventos_servidor_id_" + ThreadID + ".txt");
            logWriter = new PrintWriter(logFile);
            RegistrarEvento("LOG STARTED / SERVER " + ThreadID);
        } catch (IOException e) {
            System.err.println("ERROR CREATING LOG FOR SERVER " + ThreadID);
            e.printStackTrace();
        }

        Connection Conexao = null;
        Channel CanalTarefas = null;
        Channel CanalAtualizacoes = null;
        Channel CanalSincronizacao = null;
        Channel CanalEleicao = null;

        try {
            CarregarBancoDoServidor();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("rabbitmq");
            factory.setUsername("guest");
            factory.setPassword("guest");

            final int TentativasMaximas = 10;
            boolean isConnected = false;

            for (int attempt = 1; attempt <= TentativasMaximas; attempt++) {
                try {
                    Conexao = factory.newConnection();
                    CanalTarefas = Conexao.createChannel();
                    CanalAtualizacoes = Conexao.createChannel();
                    CanalSincronizacao = Conexao.createChannel();
                    CanalEleicao = Conexao.createChannel();

                    // Exchange da Sincronização
                    CanalSincronizacao.exchangeDeclare(SYNC_EXCHANGE, BuiltinExchangeType.FANOUT);
                    String syncQueue = CanalSincronizacao.queueDeclare().getQueue();
                    CanalSincronizacao.queueBind(syncQueue, SYNC_EXCHANGE, "");

                    // Fila de eleição
                    String NomeFilaEleicao = ELECTION_QUEUE_PREFIX + ThreadID;
                    CanalEleicao.queueDeclare(NomeFilaEleicao, false, false, false, null);

                    // Coordenador inicial
                    if (ThreadID == 1) {
                        synchronized (CoordinatorLock) {
                            if (!isCoordinator) {
                                isCoordinator = true;
                                IniciarCoordenador(CanalSincronizacao);
                                RegistrarEvento("SERVER INITIALIZED AS PRIMARY COORDINATOR");
                            }
                        }
                    }

                    EscutarCanalSincronizador(CanalSincronizacao, syncQueue);

                    final Channel CanalEleicaoFinal = CanalEleicao;
                    final Connection ConexaoFinal = Conexao;
                    final Channel CanalSincronizacaoFinal = CanalSincronizacao;

                    // Escutar mensagens de eleição
                    EscutarCanalDeEleicao(CanalEleicao, CanalEleicaoFinal, ConexaoFinal, CanalSincronizacaoFinal, NomeFilaEleicao);
                    RegistrarEvento("SUCCESSFULLY CONNECTED TO RabbitMQ ON ATTEMP #" + attempt);

                    isConnected = true;
                    break;
                } catch (Exception e) {
                    RegistrarEvento(" ATEMPT " + attempt + "/" + TentativasMaximas + " FAILED. RETRYING SHORTLY...");
                    Thread.sleep(attempt * 2000L);
                }
            }

            if (!isConnected || Conexao == null || CanalTarefas == null || CanalAtualizacoes == null) {
                RegistrarEvento("UNABLE TO CONNECT TO RabbitMQ AFTER SEVERAL ATTEMPTS");
                return;
            }

            IniciarMonitorDeEleicao(CanalEleicao, Conexao, CanalSincronizacao);

            // Tarefas normais
            CanalTarefas.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            CanalAtualizacoes.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            String updateQueue = CanalAtualizacoes.queueDeclare().getQueue();
            CanalAtualizacoes.queueBind(updateQueue, EXCHANGE_NAME, "");

            RegistrarEvento("AWAITING MESSAGES...");

            final Channel CanalTarefasFinal = CanalTarefas;
            final Channel CanalAtualizacaoFinal = CanalAtualizacoes;

            // Código AMQP
            DeliverCallback taskCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String correlationId = delivery.getProperties().getCorrelationId();
                String replyTo = delivery.getProperties().getReplyTo();

                RegistrarEvento("TASK MESSAGE: " + message);
                String response = ProcessarMensagem(message, CanalAtualizacaoFinal);

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();
                CanalTarefasFinal.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
            };

            // Código AMQP
            DeliverCallback updateCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                RegistrarEvento("UPDATE MESSAGE: " + message);
                ConsumirMensagens(message);
            };

            CanalTarefas.basicConsume(TASK_QUEUE_NAME, true, taskCallback, consumerTag -> {});
            CanalAtualizacoes.basicConsume(updateQueue, true, updateCallback, consumerTag -> {});

            while (isRunning) {
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (CanalTarefas != null) CanalTarefas.close();
                if (CanalAtualizacoes != null) CanalAtualizacoes.close();
                if (CanalSincronizacao != null) CanalSincronizacao.close();
                if (CanalEleicao != null) CanalEleicao.close();
                if (Conexao != null) Conexao.close();
                if (logWriter != null) logWriter.close();
            } catch (Exception e) {
                System.err.println("[SERVER " + ThreadID + "] FAILED TO CLOSE RESOURCES:");
                e.printStackTrace();
            }
        }
    }


    private String CriarPostagem(Map<String, Object> DadosPedido, Channel CanalAtualizacao) {
        Map<String, Object> post = MapeadorDados.convertValue(DadosPedido.get("data"), new TypeReference<>() {
        });
        String postId = "post_" + (((List<?>) Dados.get("posts")).size() + 1);
        post.put("postId", postId);

        List<Map<String, Object>> posts = (List<Map<String, Object>>) Dados.get("posts");
        posts.add(post);
        Dados.put("posts", posts);

        AtualizarArquivoBanco(post, "add", CanalAtualizacao);

        String timestampStr = post.get("timestamp").toString();
        long incomingTs = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        VerificarRelogioEAtualizar(incomingTs, false);

        return "{\"status\": \"success\", \"postId\": \"" + postId + "\"}";
    }
    
    private String GetPostagems(Map<String, Object> DadosPedido) {
        try {
            List<Map<String, Object>> posts = (List<Map<String, Object>>) Dados.get("posts");
            return MapeadorDados.writeValueAsString(posts);
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    private String SeguirUsuario(Map<String, Object> DadosPedido, Channel CanalAtualizacao) {
        Map<String, Object> followData = MapeadorDados.convertValue(DadosPedido.get("data"), new TypeReference<>() {});
        String follower = (String) followData.get("follower");
        String following = (String) followData.get("following");

        Map<String, List<String>> follows = Dados.containsKey("follows")
            ? MapeadorDados.convertValue(Dados.get("follows"), new TypeReference<>() {})
            : new HashMap<>();

        List<String> followingList = follows.getOrDefault(follower, new ArrayList<>());
        if (!followingList.contains(following)) {
            followingList.add(following);
        }
        follows.put(follower, followingList);
        Dados.put("follows", follows);

        Map<String, Object> castedFollows = MapeadorDados.convertValue(follows, new TypeReference<>() {});
        AtualizarArquivoBanco(castedFollows, "add_follow", CanalAtualizacao);

        String TimestampString = followData.get("timestamp").toString();
        long IncomingTimestamp = LocalDateTime.parse(TimestampString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        VerificarRelogioEAtualizar(IncomingTimestamp, false);

        return "{\"status\": \"success\", \"message\": \"Follow registrado com sucesso\"}";
    }

    private String GetSeguidores(Map<String, Object> DadosPedido) {
        try {        
            String username = (String) ((Map<?, ?>) DadosPedido.get("data")).get("username");

            Map<String, List<String>> follows = Dados.containsKey("follows")
                ? MapeadorDados.convertValue(Dados.get("follows"), new TypeReference<>() {})
                : new HashMap<>();

            List<String> followingList = follows.getOrDefault(username, new ArrayList<>());
            return MapeadorDados.writeValueAsString(followingList);
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        } 
    }

    private String GetHistorico(Map<String, Object> DadosPedido) {
        try {
            Map<String, Object> payload = MapeadorDados.convertValue(DadosPedido.get("data"), new TypeReference<>() {});
            String sender = (String) payload.get("sender");
            String receiver = (String) payload.get("receiver");

            List<Map<String, Object>> messages = Dados.containsKey("messages")
                ? MapeadorDados.convertValue(Dados.get("messages"), new TypeReference<>() {})
                : new ArrayList<>();

            List<Map<String, Object>> historico = messages.stream()
                .filter(m -> (sender.equals(m.get("sender")) && receiver.equals(m.get("receiver"))) ||
                            (receiver.equals(m.get("sender")) && sender.equals(m.get("receiver"))))
                .toList();

            return MapeadorDados.writeValueAsString(historico);
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    private String EnviarMensagem(Map<String, Object> DadosPedido, Channel CanalAtualizacao) {
        Map<String, Object> msg = MapeadorDados.convertValue(DadosPedido.get("data"), new TypeReference<>() {
        });

        List<Map<String, Object>> messages = Dados.containsKey("messages")
                ? MapeadorDados.convertValue(Dados.get("messages"), new TypeReference<>() {
                })
                : new ArrayList<>();

        messages.add(msg);
        Dados.put("messages", messages);

        AtualizarArquivoBanco(msg, "add_message", CanalAtualizacao);

        String timestampStr = msg.get("timestamp").toString();
        long incomingTs = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        VerificarRelogioEAtualizar(incomingTs, false);

        return "{\"status\": \"success\"}";
    }
    
    private String GetSeguidoresMutuos(Map<String, Object> DadosPedido) {
        try {
            String username = (String) ((Map<?, ?>) DadosPedido.get("data")).get("username");

            Map<String, List<String>> follows = Dados.containsKey("follows")
                ? MapeadorDados.convertValue(Dados.get("follows"), new TypeReference<>() {})
                : new HashMap<>();

            List<String> seguindo = follows.getOrDefault(username, new ArrayList<>());

            List<String> mutuos = seguindo.stream()
                .filter(user -> follows.containsKey(user) && follows.get(user).contains(username))
                .toList();

            return MapeadorDados.writeValueAsString(mutuos);
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    private String DesligarCoordenador() {
        if (isCoordinator) {
            isRunning = false;
            RegistrarEvento("COORDINATOR TERMINATED DUE TO FANOUT MESSAGE");
            PararCoordenador();
            return "{\"status\": \"ok\", \"message\": \"COORD_SHUTDOWN_FANOUT\"}";
        } else {
            RegistrarEvento("NOT A COORDINATOR SERVER");
            return "{\"status\": \"ok\", \"message\": \"IGNORED_SHUTDOWN\"}";
        }
    }

    private String ProcessarMensagem(String Mensagem, Channel CanalAtualizacao) {
        try {
            Map<String, Object> DadosPedido = MapeadorDados.readValue(Mensagem, new TypeReference<>() {});
            String Operacao = (String) DadosPedido.get("operation");

            String Resposta = switch (Operacao) {
                case "criar_post" -> CriarPostagem(DadosPedido, CanalAtualizacao);
                case "get_posts" -> GetPostagems(DadosPedido);
                case "seguir" -> SeguirUsuario(DadosPedido, CanalAtualizacao);
                case "get_seguidores" -> GetSeguidores(DadosPedido);
                case "get_historico" -> GetHistorico(DadosPedido);
                case "enviar_mensagem" -> EnviarMensagem(DadosPedido, CanalAtualizacao);
                case "get_seguidores_em_comum" -> GetSeguidoresMutuos(DadosPedido);
                case "desligar_coordenador" -> DesligarCoordenador();
                default -> "{\"status\": \"error\", \"message\": \"Nenhuma operação encontrada.\"}";
            };

            return Resposta;

        } catch (Exception e) {
            e.printStackTrace();
            return "{\"status\": \"error\", \"message\": \"Failed to process message\"}";
        }
    }

    private void AtualizarArquivoBanco(Map<String, Object> entry, String operation, Channel updateChannel) {
        try {
            MapeadorDados.writeValue(new File(ArquivoJSON), Dados);
            PublicarAtualizacao(updateChannel, entry, operation);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void PublicarAtualizacao(Channel updateChannel, Map<String, Object> entry, String operation) {
        try {
            Map<String, Object> updateMessage = new HashMap<>();
            updateMessage.put("operation", operation);
            updateMessage.put("data", entry);
            updateMessage.put("sourceThreadId", ThreadID);

            String jsonData = MapeadorDados.writeValueAsString(updateMessage);

            updateChannel.basicPublish(EXCHANGE_NAME, "", null, jsonData.getBytes(StandardCharsets.UTF_8));
            RegistrarEvento("PUSHED UPDATE: " + jsonData);
        } catch (Exception e) {
            System.err.println("[SERVER " + ThreadID + "] FAILED TO PUSH UPDATE:");
            e.printStackTrace();
        }
    }

    private void AdicionarPost(Map<String, Object> entry) {
        if (!entry.containsKey("postId")) {
            return;
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> Posts = (List<Map<String, Object>>) Dados.get("posts");
        Posts.add(entry);
        RegistrarEvento("POST REPLICATED SUCCESSFULLY");
    }
    
    private void AdicionarSeguidor(Map<String, Object> entry) {
        Map<String, List<String>> FollowsRecebidos = MapeadorDados.convertValue(entry,
                new TypeReference<Map<String, List<String>>>() {
                });
        Dados.put("follows", FollowsRecebidos);
    }
    
    private void AdicionarMensagem(Map<String, Object> entry) {
        Map<String, Object> Mensagem = (Map<String, Object>) entry;
        List<Map<String, Object>> Mensagens = Dados.containsKey("messages")
                ? MapeadorDados.convertValue(Dados.get("messages"), new TypeReference<>() {
                })
                : new ArrayList<>();

        Mensagens.add(Mensagem);
        Dados.put("messages", Mensagens);
    }

    private void ConsumirMensagens(String jsonData) {
        try {
            
            Map<String, Object> MensagemAtualizacao = MapeadorDados.readValue(jsonData, new TypeReference<Map<String, Object>>() {});
            String TipoOperacao = (String) MensagemAtualizacao.get("operation");
            int IdServidorOriginario = (int) MensagemAtualizacao.get("sourceThreadId"); 
            Map<String, Object> entry = MapeadorDados.convertValue(MensagemAtualizacao.get("data"), new TypeReference<Map<String, Object>>() {});

            if (IdServidorOriginario == ThreadID) {
                RegistrarEvento("SELF-UPDATE DETECTED. IGNORED.");
                return;
            }

            switch (TipoOperacao) {
                case "add" -> AdicionarPost(entry);
                case "add_follow" -> AdicionarSeguidor(entry);
                case "add_message" -> AdicionarMensagem(entry);
            }

            MapeadorDados.writeValue(new File(ArquivoJSON), Dados);
            RegistrarEvento("SERVER DATABASE UPDATED SUCCESSFULLY");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void CriarArquivoBancoServidor(File DirBanco) {
        boolean created = DirBanco.mkdirs();
        if (created) {
            RegistrarEvento("CREATED /data DIRECTORY");
        } else {
            RegistrarEvento("UNABLE TO CREATE /data DIRECTORY");
        }
    }

    private void CarregarDadosServidor(File ArquivoBanco) {
        try {
            Map<String, Object> loadedData = MapeadorDados.readValue(ArquivoBanco, new TypeReference<Map<String, Object>>() {});
            Dados.putAll(loadedData);
            RegistrarEvento("SERVER DATA LOADED SUCCESSFULLY " + Dados);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void CarregarBancoDoServidor() {
        File DirBanco = new File("/data");
        if (!DirBanco.exists()) {
            CriarArquivoBancoServidor(DirBanco);
        }

        File file = new File(ArquivoJSON);
        if (file.exists()) {
            CarregarDadosServidor(file);
        } else {
            // Inicia com listas vazias
            Dados.put("posts", new ArrayList<Map<String, Object>>());
            Dados.put("users", new ArrayList<Map<String, Object>>());
            RegistrarEvento("NO EXISTING DATA FOUND. INITIALIZING EMPTY DATA.");
        }
    }

    public static void main(String[] args) {
        
        for (int i = 1; i <= SERVER_COUNT; i++) {
            new Thread(new Main(i), "Servidor ID_" + i).start();
        }

        // Cria um estado de stay-alive na Thread principal
        synchronized (Main.class) {
            try {
                Main.class.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }

    private void AtualizarRelogio() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    double ChanceDesvio = Math.random();
                    if (ChanceDesvio < 0.15) {
                        int offset = -1;
                        RelogioLogico += offset;
                        RegistrarEvento("OFFSET SERVER'S CLOCK BY " + offset + "s → " + RelogioLogico);
                    } else {
                        RelogioLogico += 1;
                    }
                } catch (InterruptedException e) {
                    break; // Encerra a thread quando interrompida
                }
            }
        }, "AtualizadorRelogio-" + ThreadID).start();
    }

    private void VerificarRelogioEAtualizar(long TimestampCoordenador, boolean isForced) {
        if (isForced || TimestampCoordenador > RelogioLogico) {
            if (isForced) {
                RegistrarEvento("CLOCK UPDATE FROM COORDINATOR: FROM " + RelogioLogico + " TO " + TimestampCoordenador);
            } else {
                RegistrarEvento("CLOCK UPDATE FROM MESSAGE: FROM " + RelogioLogico + " TO " + TimestampCoordenador);
            }
            RelogioLogico = TimestampCoordenador;
        }
    }

    private void LoopCoordenadora(Channel syncChannel) {
        while (isRunning && !Thread.currentThread().isInterrupted() && isCoordinator) {
            try {

                Thread.sleep(5000);
                if (isCoordinator) { // Verificar novamente pois pode ter mudado durante o sleep
                    
                    syncChannel.basicPublish(SYNC_EXCHANGE, "", null, String.valueOf(RelogioLogico).getBytes());
                    RegistrarEvento("SENDING COORDINATOR CLOCK (" + RelogioLogico + ") TO SYNC.");

                } else {

                    RegistrarEvento("NO LONGER COORDINATOR. ENDING CLOCK SYNC CYCLE.");
                    break;
                }

            } catch (InterruptedException e) {

                RegistrarEvento("COORDINATOR INTERRUPTED.");
                break;

            } catch (Exception e) {

                RegistrarEvento("FAILED TO SEND SYNC CLOCK " + e.getMessage());
                break;

            }
        }
        RegistrarEvento("COORDINATOR THREAD ENDED");
    }

    private void IniciarCoordenador(Channel syncChannel) {
        synchronized (CoordinatorLock) {

            if (CoordinatorThread != null && CoordinatorThread.isAlive()) {
                RegistrarEvento("COORDINATOR THREAD ALREADY IN EXECUTION");
                return;
            }
            
            RegistrarEvento("STARTING NEW COORDINATOR THREAD");
            
            CoordinatorThread = new Thread(() -> {
                LoopCoordenadora(syncChannel);
            }, "Coordinator-" + ThreadID);

            CoordinatorThread.start();
        }
    }

    private void PararCoordenador() {
        synchronized (CoordinatorLock) {
            if (CoordinatorThread != null && CoordinatorThread.isAlive()) {
                CoordinatorThread.interrupt();
                RegistrarEvento("COORDINATOR THREAD INTERRUPTED");
                try {
                    CoordinatorThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                CoordinatorThread = null;
            }
        }
    }

    private void IniciarMonitorDeEleicao(Channel CanalEleicao, Connection Conexao, Channel CanalSincronizacao) {
        new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(3000);
                    
                    if (isCoordinator) {
                        continue;
                    }
                    
                    long TempoPassado = System.currentTimeMillis() - LastCoordinatorPing;

                    if (TempoPassado <= ElectionTriggerTimeout) {
                        continue;
                    }

                    if (ElectionInProgress.get()) {
                        continue;
                    }

                    if (AwaitingElectionResponse) {
                        continue;
                    }

                    RegistrarEvento("COORDINATOR HAS BEEN INACTIVE FOR " + (TempoPassado / 1000) + "s. PROMPTING ELECTION.");
                                            
                    // Tentar marcar como em progresso, se conseguir, inicio
                    if (ElectionInProgress.compareAndSet(false, true)) {
                        AwaitingElectionResponse = true;
                        IniciarEleicao(CanalEleicao, Conexao, CanalSincronizacao);
                    } else {
                        RegistrarEvento("ANOTHER SERVER HAS ALREADY BEGUN AN ELECTION. AWAITING.");
                    }
                    
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    RegistrarEvento("ELECTION MONITOR FAILED: " + e.getMessage());
                }
            }
        }, "ElectionMonitor-" + ThreadID).start();
    }

    private void IniciarEleicao(Channel Canal, Connection Conexao, Channel CanalSincronizacao) {
        try {
            List<Integer> ServidoresSuperiores = new ArrayList<>();

            for (int i = ThreadID + 1; i <= SERVER_COUNT; i++) {
                String queue = ELECTION_QUEUE_PREFIX + i;
                try {
                    String msg = "ELECTION_FROM_" + ThreadID;
                    Canal.basicPublish("", queue, null, msg.getBytes());
                    RegistrarEvento("SENT ELECTION REQUEST TO SERVER " + i);
                    ServidoresSuperiores.add(i);
                } catch (Exception e) {
                    RegistrarEvento("FAILED TO CONTACT SERVER " + i);
                }
            }

            if (ServidoresSuperiores.isEmpty()) {
                RegistrarEvento("NO SERVER WITH SUPERIOR ID FROM SERVER " + ThreadID + ". ELECTED COORDINATOR.");
                AnunciarNovoCoordenador(Conexao, CanalSincronizacao);
                return;
            }

            // Espera resposta por 5 segundos
            new Thread(() -> {
                try {
                    Thread.sleep(5000);
                    if (AwaitingElectionResponse && ElectionInProgress.get()) {
                        RegistrarEvento("ELECTION TIMEOUT. NO SERVERS REPLIED. ELECTED COORDINATOR.");
                        AnunciarNovoCoordenador(Conexao, CanalSincronizacao);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "ElectionTimeout-" + ThreadID).start();

        } catch (Exception e) {
            RegistrarEvento("FAILED TO START ELECTION: " + e.getMessage());
            ElectionInProgress.set(false);
            AwaitingElectionResponse = false;
        }
    }

    private void AnunciarNovoCoordenador(Connection Conexao, Channel CanalSincronizacao) {
        try (Channel Canal = Conexao.createChannel()) {
            String msg = "NEW_COORDINATOR_" + ThreadID;

            for (int i = 1; i <= SERVER_COUNT; i++) {
                String queue = ELECTION_QUEUE_PREFIX + i;
                try {
                    Canal.basicPublish("", queue, null, msg.getBytes());
                    RegistrarEvento("NOTIFYING SERVER " + i + " ABOUT NEW COORDINATOR");
                } catch (Exception e) {
                    RegistrarEvento("FAILED TO NOTIFY SERVER " + i);
                }
            }

            synchronized (CoordinatorLock) {
                isCoordinator = true;
                AwaitingElectionResponse = false;
                ElectionInProgress.set(false);
                LastCoordinatorPing = System.currentTimeMillis();
                
                RegistrarEvento("THIS SERVER HAS BEEN ELECTED COORDINATOR.");
                IniciarCoordenador(CanalSincronizacao);
            }

        } catch (Exception e) {
            RegistrarEvento("FAILED TO ANNOUNCE NEW COORDINATOR: " + e.getMessage());
            ElectionInProgress.set(false);
            AwaitingElectionResponse = false;
        }
    }

    private void RegistrarEvento(String message) {
        String Timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String LogFinal = "[" + Timestamp + "] [Servidor " + ThreadID + "] " + message;

        if (logWriter != null) {
            logWriter.println(LogFinal);
            logWriter.flush();
        }
    }
}