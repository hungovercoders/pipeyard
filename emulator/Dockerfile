# AWS Emulator (https://github.com/aws/aws-emulator), installed from AWS's own package.
# AWS publishes packages, not a container image, so this builds one.
FROM amazonlinux:2023
ARG TARGETARCH
RUN arch=$([ "$TARGETARCH" = "arm64" ] && echo aarch64 || echo x86_64) \
 && dnf install -y \
      "https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/latest/aws-emulator-${arch}.rpm" \
 && dnf clean all \
 && mkdir -p /home/emulator && chown 1000:1000 /home/emulator
ENV HOME=/home/emulator
USER 1000:1000
EXPOSE 9297
ENTRYPOINT ["aws-emulator", "--bind", "0.0.0.0"]
