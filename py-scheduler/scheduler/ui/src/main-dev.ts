import { mount } from "svelte";
import AppDev from "./AppDev.svelte";
import "./app.css";

mount(AppDev, { target: document.getElementById("app")! });
