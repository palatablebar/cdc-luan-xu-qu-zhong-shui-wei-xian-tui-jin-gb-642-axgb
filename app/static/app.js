/* 交互：采信 / 重跑 / 物化 / 续跑 / 试归一。只做请求与提示，不做业务判断。 */
(function () {
  "use strict";

  var toastTimer = null;

  function toast(message, bad) {
    var box = document.getElementById("toast");
    if (!box) return;
    box.textContent = message;
    box.classList.toggle("is-bad", !!bad);
    box.classList.add("is-on");
    if (toastTimer) window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(function () {
      box.classList.remove("is-on");
    }, 4200);
  }

  function request(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {})
    }).then(function (response) {
      return response.text().then(function (text) {
        var data = {};
        try {
          data = text ? JSON.parse(text) : {};
        } catch (err) {
          data = { message: text };
        }
        if (!response.ok) {
          var message = data.message || data.error || ("HTTP " + response.status);
          throw new Error(message);
        }
        return data;
      });
    });
  }

  function busy(button, on) {
    if (!button) return;
    button.disabled = on;
    if (on) {
      button.setAttribute("data-label", button.textContent);
      button.textContent = "运行中";
    } else if (button.getAttribute("data-label")) {
      button.textContent = button.getAttribute("data-label");
    }
  }

  function afterRun(data) {
    if (data.idempotent_replay) {
      toast("命中已有批次 " + data.batch_id);
    } else {
      toast("批次 " + data.batch_id + " · 已应用 " + (data.counters.applied || 0));
    }
    window.setTimeout(function () {
      window.location.reload();
    }, 650);
  }

  function settleDate() {
    var box = document.getElementById("settle-date");
    return box ? box.value : "";
  }

  function reloadSoon() {
    window.setTimeout(function () {
      window.location.reload();
    }, 650);
  }

  document.addEventListener("click", function (event) {
    var target = event.target.closest("button");
    if (!target) return;

    if (target.id === "btn-import" || target.id === "btn-rerun") {      var select = document.getElementById("source-select");
      var source = target.id === "btn-rerun" ? "all" : (select ? select.value : "all");
      busy(target, true);
      request("/api/events/import", { source: source })
        .then(afterRun)
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
      return;
    }

    if (target.id === "btn-snapshot") {
      busy(target, true);
      request("/api/snapshot", {})
        .then(function (data) {
          toast("快照 " + data.rows + " 行 · as_of " + data.as_of);
          window.setTimeout(function () {
            window.location.reload();
          }, 650);
        })
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
      return;
    }

    if (target.id === "btn-resume") {
      var batchId = target.getAttribute("data-batch");
      if (!batchId) return;
      busy(target, true);
      request("/api/batches/" + encodeURIComponent(batchId) + "/resume", {})
        .then(function (data) {
          toast(data.idempotent_replay ? "复用批次 " + data.batch_id
            : "续跑完成 " + data.batch_id);
          window.setTimeout(function () {
            window.location.reload();
          }, 650);
        })
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
      return;
    }

    if (target.id === "btn-settle-compute") {
      busy(target, true);
      request("/api/settlement/run", { settle_date: settleDate() })
        .then(function (data) {
          toast("已计算 " + data.run.run_id + " · 订单 " +
            (data.preview ? data.preview.added + data.preview.changed : 0) +
            " · 发布前请看影响面");
          reloadSoon();
        })
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
      return;
    }

    if (target.id === "btn-settle-publish") {
      var runId = target.getAttribute("data-run");
      var digest = target.getAttribute("data-digest");
      if (!runId || !digest) return;
      busy(target, true);
      request("/api/settlement/" + encodeURIComponent(runId) + "/publish",
        { impact_digest: digest })
        .then(function (data) {
          var result = data.result || {};
          toast(result.noop ? "与已发布版本一致，无需发布"
            : "已发布 " + data.run.run_id + " · 影响 " +
              (result.diff ? result.diff.changed : 0) + " 单");
          reloadSoon();
        })
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
      return;
    }

    if (target.id === "btn-preview") {
      var input = document.getElementById("preview-input");
      var state = document.getElementById("preview-state");
      if (!input) return;
      var payload;
      try {
        payload = JSON.parse(input.value);
      } catch (err) {
        toast("JSON 解析失败", true);
        return;
      }
      busy(target, true);
      request("/api/schema/preview", { event: payload })
        .then(function (data) {
          if (state) state.textContent = data.ok ? "通过" : "隔离";
          var rows = [
            ["table", data.table],
            ["pk", data.pk],
            ["version", data.version],
            ["event_time", data.event_time],
            ["amount", data.amount],
            ["codes", (data.codes || []).join(", ") || "无"]
          ];
          var body = document.querySelector("#preview-table tbody");
          if (body) {
            body.innerHTML = "";
            rows.forEach(function (pair) {
              var tr = document.createElement("tr");
              var left = document.createElement("td");
              left.className = "mono";
              left.textContent = pair[0];
              var right = document.createElement("td");
              right.className = "mono";
              right.textContent = pair[1] === null || pair[1] === undefined
                ? "—" : String(pair[1]);
              tr.appendChild(left);
              tr.appendChild(right);
              body.appendChild(tr);
            });
          }
          toast(data.ok ? "归一通过" : "该行会被隔离");
          busy(target, false);
        })
        .catch(function (err) {
          toast(err.message, true);
          busy(target, false);
        });
    }
  });
})();
