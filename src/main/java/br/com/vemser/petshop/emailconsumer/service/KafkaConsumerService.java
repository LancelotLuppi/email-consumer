package br.com.vemser.petshop.emailconsumer.service;

import br.com.vemser.petshop.emailconsumer.dto.ClienteEmailMessageDTO;
import br.com.vemser.petshop.emailconsumer.enums.TipoRequisicao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final ObjectMapper objectMapper;
    private final EmailService emailService;

    @KafkaListener(
            topics = "${kafka.email-topic}",
            groupId = "${kafka.client-id}",
            containerFactory = "listenerContainerFactory",
            clientIdPrefix = "chat"
    )
    public void enviarEmail(@Payload String payload,
                            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                            @Header(KafkaHeaders.OFFSET) Long offset) throws JsonProcessingException {
        ClienteEmailMessageDTO dadosEnvioEmail = objectMapper.readValue(payload, ClienteEmailMessageDTO.class);
        if(partition == TipoRequisicao.POST.ordinal()) {
            log.info("ENVIANDO EMAIL TIPO POST");

            emailService.sendEmail(
                    dadosEnvioEmail.getNome(),
                    dadosEnvioEmail.getId(),
                    dadosEnvioEmail.getEmail(),
                    TipoRequisicao.POST);

            getLog(dadosEnvioEmail);
        } else if(partition == TipoRequisicao.PUT.ordinal()) {
            log.info("ENVIANDO EMAIL TIPO PUT");

            emailService.sendEmail(
                    dadosEnvioEmail.getNome(),
                    dadosEnvioEmail.getId(),
                    dadosEnvioEmail.getEmail(),
                    TipoRequisicao.PUT);

            getLog(dadosEnvioEmail);
        } else if(partition == TipoRequisicao.DELETE.ordinal()) {
            log.info("ENVIANDO EMAIL TIPO DELETE");

            emailService.sendEmail(
                    dadosEnvioEmail.getNome(),
                    dadosEnvioEmail.getId(),
                    dadosEnvioEmail.getEmail(),
                    TipoRequisicao.DELETE);

            getLog(dadosEnvioEmail);
        }
    }

    private void getLog(ClienteEmailMessageDTO dadosEnvioEmail) {
        log.info("Email enviado com SUCESSO, dados cliente: id -> ["
                +  dadosEnvioEmail.getId() + "], nome -> ["
                + dadosEnvioEmail.getNome() + "], email -> ["
                + dadosEnvioEmail.getEmail() + "]");
    }
}
