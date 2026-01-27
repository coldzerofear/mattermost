FROM mattermost/mattermost-enterprise-edition:11.2.1

USER root

# 覆盖 Mattermost 服务端二进制
COPY --chown=2000:2000 ./server/bin/mattermost /mattermost/bin/mattermost
COPY --chown=2000:2000 ./server/bin/mmctl /mattermost/bin/mmctl

# 清理旧的前端资源并替换为自定义构建
#RUN rm -rf /mattermost/client
#COPY --chown=2000:2000 ./webapp/channels/dist /mattermost/client
COPY ./license-gen.txt /mattermost-license/license

USER mattermost
